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
  // closes the channels when the layer is released, or right away if it already was
  scope: Scope,
  connections: Ref.Synchronized[Map[PodAddress, PodClient]]
) extends Pods {

  private def getConnection(pod: PodAddress): Task[PodClient] =
    // optimize happy path and only get first
    connections.get.flatMap(_.get(pod) match {
      case Some(client) => ZIO.succeed(client)
      case None         =>
        // then do modify in the case it doesn't already exist
        connections.modifyZIO { map =>
          map.get(pod) match {
            case Some(client) => ZIO.succeed((client, map))
            case None         =>
              scope
                .extend(
                  ZIO.logDebug(s"Opening connection to pod $pod") *>
                    ZIO.addFinalizer(ZIO.logDebug(s"Closing connection to pod $pod")) *>
                    GrpcPods.clientBackend(pod.host, pod.port, config)
                )
                .map { backend =>
                  val client = new PodClient(backend)
                  (client, map.updated(pod, client))
                }
          }
        }.uninterruptible
    })

  private def mapClientError(pod: PodAddress, streamedReplies: Boolean)(ex: StatusException): Throwable =
    ex.getStatus.getCode match {
      case Status.Code.CANCELLED if streamedReplies        => StreamCancelled
      case Status.Code.UNAVAILABLE | Status.Code.CANCELLED => PodUnavailable(pod)
      case _                                               => ex
    }

  def assignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.assignShards(AssignShardsRequest(shards.toList)).unit)

  def unassignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit] =
    getConnection(pod).flatMap(_.unassignShards(UnassignShardsRequest(shards.toList)).unit)

  def ping(pod: PodAddress): Task[Unit] =
    getConnection(pod).flatMap(_.pingShards(PingShardsRequest()).unit)

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
        scope       <- ZIO.serviceWithZIO[Scope](_.forkWith(ExecutionStrategy.Parallel))
        connections <- Ref.Synchronized.make(Map.empty[PodAddress, PodClient])
      } yield new GrpcPods(config, scope, connections)
    }
}
