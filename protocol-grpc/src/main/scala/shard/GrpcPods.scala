package shard

import shard.errors._
import shard.interfaces.Pods
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.protocol.Sharding._
import io.grpc.{ ClientInterceptors, ManagedChannelBuilder, Status, StatusException }
import proteus.client.ZioClientBackend
import zio._
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

class GrpcPods(
  config: GrpcConfig,
  connections: Ref.Synchronized[Map[PodAddress, (PodClient, Fiber[Throwable, Nothing])]]
) extends Pods {

  private def getConnection(pod: PodAddress): Task[PodClient] =
    // optimize happy path and only get first
    connections.get.flatMap(_.get(pod) match {
      case Some((client, _)) => ZIO.succeed(client)
      case None              =>
        // then do modify in the case it doesn't already exist
        connections.modifyZIO { map =>
          map.get(pod) match {
            case Some((client, _)) => ZIO.succeed((client, map))
            case None              =>
              // create a fiber that never ends and keeps the connection alive
              for {
                _       <- ZIO.logDebug(s"Opening connection to pod $pod")
                promise <- Promise.make[Throwable, PodClient]
                fiber   <- ZIO
                             .scoped[Any] {
                               GrpcPods
                                 .clientBackend(pod.host, pod.port, config)
                                 .flatMap(backend => promise.succeed(PodClient.from(backend)) *> ZIO.never)
                             }
                             .onError(promise.failCause(_))
                             .ensuring(
                               connections.update(_ - pod) *> ZIO.logDebug(s"Closed connection to pod $pod")
                             )
                             .forkDaemon
                client  <- promise.await
              } yield (client, map.updated(pod, (client, fiber)))
          }
        }
    })

  private def mapClientError(pod: PodAddress, isStream: Boolean)(t: Throwable): Throwable =
    t match {
      case ex: StatusException if ex.getStatus.getCode == Status.Code.UNAVAILABLE => PodUnavailable(pod)
      case ex: StatusException if ex.getStatus.getCode == Status.Code.CANCELLED   =>
        if (isStream) StreamCancelled else PodUnavailable(pod)
      case other                                                                  => other
    }

  def assignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.assignShards(AssignShardsRequest(shards.toList)).unit)

  def unassignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.unassignShards(UnassignShardsRequest(shards.toList)).unit)

  def ping(pod: PodAddress): Task[Unit] =
    getConnection(pod).flatMap(_.pingShards(PingShardsRequest()).unit)

  def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply] =
    getConnection(pod).flatMap { conn =>
      conn
        .send(message)
        .mapBoth(mapClientError(pod, isStream = false), _.reply)
    }

  def sendStream(pod: PodAddress, messages: ZStream[Any, Nothing, BinaryMessage]): Task[BinaryReply] =
    getConnection(pod).flatMap { conn =>
      conn
        .sendStream(messages)
        .mapBoth(mapClientError(pod, isStream = false), _.reply)
    }

  def sendStreamAndReceiveStream(
    pod: PodAddress,
    messages: ZStream[Any, Nothing, BinaryMessage]
  ): ZStream[Any, Throwable, BinaryReply] =
    ZStream
      .fromZIO(getConnection(pod))
      .flatMap(
        _.sendStreamAndReceiveStream(messages)
          .mapBoth(mapClientError(pod, isStream = true), _.reply)
      )
}

object GrpcPods {

  /**
   * Open a gRPC client to the given host and port, closed when the scope ends.
   */
  private[shard] def clientBackend(host: String, port: Int, config: GrpcConfig): RIO[Scope, ZioClientBackend] =
    ZIO.suspend {
      val builder =
        ManagedChannelBuilder.forAddress(host, port).maxInboundMessageSize(config.maxInboundMessageSize).usePlaintext()
      config.executor.foreach(builder.executor)
      ZIO
        .acquireRelease(ZIO.attempt(builder.build())) { channel =>
          ZIO.attemptBlocking {
            channel.shutdown()
            channel.awaitTermination(config.shutdownTimeout.toMillis, TimeUnit.MILLISECONDS)
            channel.shutdownNow(): Unit
          }.ignore
        }
        .map(channel =>
          ZioClientBackend(ClientInterceptors.intercept(channel, config.clientInterceptors*), config.streamingPrefetch)
        )
    }

  /**
   * A layer that creates an instance of Pods that communicates using the gRPC protocol.
   */
  val live: ZLayer[GrpcConfig, Throwable, Pods] =
    ZLayer.scoped {
      for {
        config      <- ZIO.service[GrpcConfig]
        connections <- Ref.Synchronized
                         .make(Map.empty[PodAddress, (PodClient, Fiber[Throwable, Nothing])])
                         .withFinalizer(
                           // stop all connection fibers on release
                           _.get.flatMap(conns => ZIO.foreachDiscard(conns) { case (_, (_, fiber)) => fiber.interrupt })
                         )
      } yield new GrpcPods(config, connections)
    }
}
