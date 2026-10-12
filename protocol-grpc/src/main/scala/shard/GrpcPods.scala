package shard

import shard.errors._
import shard.interfaces.Pods
import shard.interfaces.Pods.BinaryMessage
import shard.protocol.Sharding._
import io.grpc.{ ClientInterceptors, ManagedChannel, ManagedChannelBuilder, Status, StatusException }
import proteus.client.ZioClientBackend
import zio._
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

class GrpcPods(
  config: GrpcConfig,
  connections: Ref.Synchronized[Map[PodAddress, GrpcPods.Connection]]
) extends Pods {
  import GrpcPods.Connection

  private val currentTimeMillis: UIO[Long] = Clock.currentTime(TimeUnit.MILLISECONDS)

  private def getConnection(pod: PodAddress): Task[Connection] =
    // optimize happy path and only get first
    connections.get.flatMap(_.get(pod) match {
      case Some(connection) => ZIO.succeed(connection)
      case None             => getOrCreateConnection(pod)
    })

  private def getOrCreateConnection(pod: PodAddress): Task[Connection] =
    // do modify in the case it doesn't already exist
    connections.modifyZIO { map =>
      map.get(pod) match {
        case Some(connection) => ZIO.succeed((connection, map))
        case None             =>
          val builder =
            config.executor match {
              case Some(executor) =>
                ManagedChannelBuilder
                  .forAddress(pod.host, pod.port)
                  .executor(executor)
                  .maxInboundMessageSize(config.maxInboundMessageSize)
                  .usePlaintext()
              case None           =>
                ManagedChannelBuilder
                  .forAddress(pod.host, pod.port)
                  .maxInboundMessageSize(config.maxInboundMessageSize)
                  .usePlaintext()
            }

          val acquireChannel: RIO[Scope, ManagedChannel] =
            ZIO.acquireRelease(ZIO.attempt(builder.build())) { channel =>
              ZIO.attemptBlocking {
                channel.shutdown()
                channel.awaitTermination(config.shutdownTimeout.toMillis, TimeUnit.MILLISECONDS)
                channel.shutdownNow(): Unit
              }.ignore
            }

          // create a fiber that never ends and keeps the connection alive
          for {
            _          <- ZIO.logDebug(s"Opening connection to pod $pod")
            promise    <- Promise.make[Throwable, PodClient]
            fiber      <- ZIO
                            .scoped[Any] {
                              acquireChannel.flatMap { rawChannel =>
                                val channel =
                                  if (config.clientInterceptors.isEmpty) rawChannel
                                  else ClientInterceptors.intercept(rawChannel, config.clientInterceptors*)
                                val backend = ZioClientBackend(channel, config.streamingPrefetch)
                                val client  = PodClient.from(backend)
                                promise.succeed(client) *> ZIO.never
                              }
                            }
                            .onError(promise.failCause(_))
                            .ensuring(
                              // remove the entry unless it was already replaced by a newer connection
                              ZIO.fiberIdWith(fiberId =>
                                connections.update(map =>
                                  map.get(pod) match {
                                    case Some(connection) if connection.fiber.id != fiberId => map
                                    case _                                                  => map - pod
                                  }
                                )
                              ) *> ZIO.logDebug(s"Closed connection to pod $pod")
                            )
                            .interruptible // callers may open the connection from an uninterruptible region
                            .forkDaemon
            client     <- promise.await
            inFlight   <- Ref.make(0)
            now        <- currentTimeMillis
            lastUsedAt <- Ref.make(now)
            connection  = Connection(client, fiber, inFlight, lastUsedAt)
          } yield (connection, map.updated(pod, connection))
      }
    }

  /**
   * Marks a connection as in use so that it is not closed while a call is running.
   * If it is being closed for being idle, a new connection is opened instead.
   */
  private def acquire(pod: PodAddress): Task[Connection] =
    getConnection(pod).flatMap(acquireOrReconnect(pod, _))

  private def acquireOrReconnect(pod: PodAddress, connection: Connection): Task[Connection] =
    connection.inFlight.modify(n => if (n < 0) (false, n) else (true, n + 1)).flatMap {
      case true  => ZIO.succeed(connection)
      case false => getOrCreateConnection(pod).flatMap(acquireOrReconnect(pod, _))
    }

  private def release(connection: Connection): UIO[Unit] =
    connection.inFlight.update(_ - 1) *> currentTimeMillis.flatMap(connection.lastUsedAt.set)

  private def withConnection[A](pod: PodAddress)(f: PodClient => Task[A]): Task[A] =
    ZIO.acquireReleaseWith(acquire(pod))(release)(connection => f(connection.client))

  private def streamWithConnection[A](pod: PodAddress)(
    f: PodClient => ZStream[Any, Throwable, A]
  ): ZStream[Any, Throwable, A] =
    ZStream.acquireReleaseWith(acquire(pod))(release).flatMap(connection => f(connection.client))

  /**
   * Closes the connections that have not been used for `connectionIdleTimeout`, for example to pods that left.
   */
  private[shard] val closeIdleConnections: UIO[Unit] =
    currentTimeMillis.flatMap { now =>
      connections.modifyZIO { map =>
        ZIO
          .filter(map.toList) { case (_, connection) =>
            connection.lastUsedAt.get.flatMap(lastUsedAt =>
              // only close connections that are not in use, and mark them as closing so that no new call starts
              if (now - lastUsedAt >= config.connectionIdleTimeout.toMillis)
                connection.inFlight.modify(n => if (n == 0) (true, -1) else (false, n))
              else ZIO.succeed(false)
            )
          }
          .map(idle => (idle, map -- idle.map(_._1)))
      }
        .flatMap(ZIO.foreachParDiscard(_) { case (pod, connection) =>
          ZIO.logDebug(s"Closing idle connection to pod $pod") *> connection.fiber.interrupt
        })
    }

  private def mapClientError(pod: PodAddress, entityId: String, isStream: Boolean)(t: Throwable): Throwable =
    t match {
      case ex: StatusException if ex.getStatus.getCode == Status.Code.RESOURCE_EXHAUSTED =>
        // entity is not managed by this pod, wait and retry (assignments will be updated)
        EntityNotManagedByThisPod(entityId)
      case ex: StatusException if ex.getStatus.getCode == Status.Code.UNAVAILABLE        => PodUnavailable(pod)
      case ex: StatusException if ex.getStatus.getCode == Status.Code.CANCELLED          =>
        if (isStream) StreamCancelled else PodUnavailable(pod)
      case other                                                                         => other
    }

  private def toSendRequest(message: BinaryMessage): SendRequest =
    SendRequest(message.entityId, message.entityType, message.body, message.replyId)

  def assignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    withConnection(pod)(_.assignShards(AssignShardsRequest(shards.toList)).unit)

  def unassignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    withConnection(pod)(_.unassignShards(UnassignShardsRequest(shards.toList)).unit)

  def ping(pod: PodAddress): Task[Unit] =
    withConnection(pod)(_.pingShards(PingShardsRequest()).unit)

  def sendMessage(pod: PodAddress, message: BinaryMessage): Task[Option[Array[Byte]]] =
    withConnection(pod) { conn =>
      conn
        .send(toSendRequest(message))
        .mapBoth(
          mapClientError(pod, message.entityId, isStream = false),
          _.body
        )
    }

  def sendStream(
    pod: PodAddress,
    entityId: String,
    messages: ZStream[Any, Throwable, BinaryMessage]
  ): Task[Option[Array[Byte]]] =
    withConnection(pod) { conn =>
      conn
        .sendStream(messages.mapBoth(Status.INTERNAL.withCause(_).asException(), toSendRequest))
        .mapBoth(
          mapClientError(pod, entityId, isStream = true),
          _.body
        )
    }

  def sendMessageAndReceiveStream(pod: PodAddress, message: BinaryMessage): ZStream[Any, Throwable, Array[Byte]] =
    streamWithConnection(pod)(
      _.sendAndReceiveStream(toSendRequest(message))
        .mapBoth(mapClientError(pod, message.entityId, isStream = true), _.body.getOrElse(Array.emptyByteArray))
    )

  def sendStreamAndReceiveStream(
    pod: PodAddress,
    entityId: String,
    messages: ZStream[Any, Throwable, BinaryMessage]
  ): ZStream[Any, Throwable, Array[Byte]] =
    streamWithConnection(pod)(
      _.sendStreamAndReceiveStream(
        messages.mapBoth(Status.INTERNAL.withCause(_).asException(), toSendRequest)
      ).mapBoth(mapClientError(pod, entityId, isStream = true), _.body.getOrElse(Array.emptyByteArray))
    )
}

object GrpcPods {

  /**
   * An open connection to a pod. `inFlight` counts the running calls, and is set to -1 once the connection is closing.
   */
  final case class Connection(
    client: PodClient,
    fiber: Fiber.Runtime[Throwable, Nothing],
    inFlight: Ref[Int],
    lastUsedAt: Ref[Long]
  )

  /**
   * A layer that creates an instance of Pods that communicates using the gRPC protocol.
   */
  val live: ZLayer[GrpcConfig, Throwable, Pods] =
    ZLayer.scoped {
      for {
        config      <- ZIO.service[GrpcConfig]
        connections <-
          Ref.Synchronized
            .make(Map.empty[PodAddress, Connection])
            .withFinalizer(
              // stop all connection fibers on release
              _.get.flatMap(conns => ZIO.foreachDiscard(conns) { case (_, connection) => connection.fiber.interrupt })
            )
        pods         = new GrpcPods(config, connections)
        // regularly close the connections to pods that are not used anymore
        _           <- pods.closeIdleConnections.repeat(Schedule.spaced(config.connectionIdleTimeout)).forkScoped
      } yield pods
    }
}
