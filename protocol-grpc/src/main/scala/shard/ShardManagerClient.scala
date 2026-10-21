package shard

import shard.internal.LocalShardManagerClient
import shard.interfaces.ShardManagerClient
import shard.protocol.ShardManager
import shard.protocol.ShardManager._
import io.grpc.CallOptions
import proteus.client.ZioClientBackend
import zio.{ Config => _, _ }

import java.util.concurrent.TimeUnit

object ShardManagerClient {

  /**
   * A layer that creates a client for the Shard Manager API using the gRPC protocol.
   */
  val live: ZLayer[Config & GrpcConfig, Throwable, ShardManagerClient] =
    ZLayer.scoped {
      for {
        config     <- ZIO.service[Config]
        grpcConfig <- ZIO.service[GrpcConfig]
        backend    <- GrpcPods.clientBackend(config.shardManagerHost, config.shardManagerPort, grpcConfig)
      } yield new GrpcClient(backend, config.serverVersion, config.shardManagerTimeout)
    }

  /**
   * A layer that mocks the Shard Manager, useful for testing with a single pod.
   */
  val local: ZLayer[Config, Nothing, ShardManagerClient] =
    LocalShardManagerClient.live

  private class GrpcClient(backend: ZioClientBackend, serverVersion: String, timeout: Duration)
      extends ShardManagerClient {
    private val withDeadline: CallOptions => CallOptions =
      _.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS)

    private val registerCall           = backend.client(ShardManager.register, ShardManager.service, withDeadline)
    private val unregisterCall         = backend.client(ShardManager.unregister, ShardManager.service, withDeadline)
    private val notifyUnhealthyPodCall =
      backend.client(ShardManager.notifyUnhealthyPod, ShardManager.service, withDeadline)
    private val getAssignmentsCall     = backend.client(ShardManager.getAssignments, ShardManager.service, withDeadline)

    def register(podAddress: PodAddress, role: Role): Task[Unit] =
      registerCall(RegisterRequest(Pod(podAddress, serverVersion, role))).unit

    def unregister(podAddress: PodAddress): Task[Unit] =
      unregisterCall(UnregisterRequest(podAddress)).unit

    def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit] =
      ZIO.logWarning(s"Notifying Shard Manager about unhealthy pod $podAddress") *>
        notifyUnhealthyPodCall(NotifyUnhealthyPodRequest(podAddress)).unit

    def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] =
      getAssignmentsCall(GetAssignmentsRequest(role)).map(_.assignments)
  }
}
