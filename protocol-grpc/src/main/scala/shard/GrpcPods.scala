package shard

import shard.errors._
import shard.interfaces.Pods
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.protocol.Sharding._
import io.grpc.{ ClientInterceptors, ConnectivityState, ManagedChannel, ManagedChannelBuilder, Status, StatusException }
import proteus.client.ZioClientBackend
import zio._
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

class GrpcPods(
  config: GrpcConfig,
  // closes the channels when the layer is released, or right away if it already was
  scope: Scope,
  connections: Ref.Synchronized[Map[PodAddress, GrpcPods.Connection]]
) extends Pods {

  private def getConnection(pod: PodAddress): Task[PodClient] =
    connection(pod).map(_.client)

  private def connection(pod: PodAddress): Task[GrpcPods.Connection] =
    // optimize happy path and only get first
    connections.get.flatMap(_.get(pod) match {
      case Some(connection) => ZIO.succeed(connection)
      case None             =>
        // then do modify in the case it doesn't already exist
        connections.modifyZIO { map =>
          map.get(pod) match {
            case Some(connection) => ZIO.succeed((connection, map))
            case None             =>
              scope.fork.flatMap(connectionScope =>
                connectionScope
                  .extend(
                    ZIO.logDebug(s"Opening connection to pod $pod") *>
                      ZIO.addFinalizer(ZIO.logDebug(s"Closing connection to pod $pod")) *>
                      GrpcPods.channel(pod.host, pod.port, config)
                  )
                  .map { channel =>
                    val connection =
                      GrpcPods.Connection(new PodClient(GrpcPods.backend(channel, config)), channel, connectionScope)
                    (connection, map.updated(pod, connection))
                  }
              )
          }
        }.uninterruptible
    })

  private def close(pod: PodAddress, connection: GrpcPods.Connection): UIO[Unit] =
    connections.update(map => if (map.get(pod).contains(connection)) map - pod else map) *>
      connection.scope.close(Exit.unit)

  private def mapClientError(pod: PodAddress, streamedReplies: Boolean)(ex: StatusException): Throwable =
    ex.getStatus.getCode match {
      case Status.Code.CANCELLED if streamedReplies        => StreamCancelled
      case Status.Code.UNAVAILABLE | Status.Code.CANCELLED => PodUnavailable(pod)
      case _                                               => ex
    }

  def assignShards(pod: PodAddress, role: Role, registration: String, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.assignShards(AssignShardsRequest(shards.toList, role.name, registration)).unit)

  def unassignShards(pod: PodAddress, role: Role, registration: String, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.unassignShards(UnassignShardsRequest(shards.toList, role.name, registration)).unit)

  def ping(pod: PodAddress): Task[Option[String]] =
    connection(pod).flatMap(connection =>
      connection.client
        .pingShards(PingShardsRequest())
        .catchSome {
          // a channel that failed to reach an earlier pod at this address waits for its backoff to connect again
          case ex
              if ex.getStatus.getCode == Status.Code.UNAVAILABLE &&
                connection.channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE =>
            close(pod, connection) *> getConnection(pod).flatMap(_.pingShards(PingShardsRequest()))
        }
        .map(_.registration)
    )

  private def singleReply(pod: PodAddress)(response: IO[StatusException, SendResponse]): Task[BinaryReply] =
    response.mapBoth(mapClientError(pod, streamedReplies = false), _.reply)

  def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply] =
    getConnection(pod).flatMap(conn => singleReply(pod)(conn.send(message)))

  def sendStream(pod: PodAddress, messages: ZStream[Any, Nothing, BinaryMessage]): Task[BinaryReply] =
    getConnection(pod).flatMap(conn => singleReply(pod)(conn.sendStream(messages)))

  def sendStreamAndReceiveStream(
    pod: PodAddress,
    messages: ZStream[Any, Nothing, BinaryMessage]
  ): ZStream[Any, Throwable, BinaryReply] =
    ZStream
      .fromZIO(getConnection(pod))
      .flatMap(
        _.sendStreamAndReceiveStream(messages)
          .mapBoth(mapClientError(pod, streamedReplies = true), _.reply)
      )
}

object GrpcPods {

  private[shard] final case class Connection(client: PodClient, channel: ManagedChannel, scope: Scope.Closeable)

  /**
   * Open a gRPC client to the given host and port, closed when the scope ends.
   */
  private[shard] def clientBackend(host: String, port: Int, config: GrpcConfig): RIO[Scope, ZioClientBackend] =
    channel(host, port, config).map(backend(_, config))

  private def channel(host: String, port: Int, config: GrpcConfig): RIO[Scope, ManagedChannel] =
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
    }

  private def backend(channel: ManagedChannel, config: GrpcConfig): ZioClientBackend =
    ZioClientBackend(ClientInterceptors.intercept(channel, config.clientInterceptors*), config.streamingPrefetch)

  /**
   * A layer that creates an instance of Pods that communicates using the gRPC protocol.
   */
  val live: ZLayer[GrpcConfig, Throwable, Pods] =
    ZLayer.scoped {
      for {
        config      <- ZIO.service[GrpcConfig]
        scope       <- ZIO.serviceWithZIO[Scope](_.forkWith(ExecutionStrategy.Parallel))
        connections <- Ref.Synchronized.make(Map.empty[PodAddress, Connection])
      } yield new GrpcPods(config, scope, connections)
    }
}
