package shard

import shard.protocol.{ ShardManager => Protocol }
import shard.protocol.ShardManager._
import io.grpc.{ ServerServiceDefinition, StatusException }
import io.grpc.protobuf.services.HealthStatusManager
import proteus.server.{ GrpcContext, ServerService, ZioServerBackend }
import zio._

object Server {

  /**
   * Start a gRPC server that exposes the Shard Manager API.
   */
  def run: RIO[ShardManager & ManagerConfig & GrpcConfig, Nothing] =
    ZIO.scoped {
      for {
        config       <- ZIO.service[ManagerConfig]
        grpcConfig   <- ZIO.service[GrpcConfig]
        shardManager <- ZIO.service[ShardManager]
        health        = new HealthStatusManager()
        _            <- GrpcShardingService.serve(config.apiPort, grpcConfig)(backend =>
                          List(buildServiceDefinition(shardManager, backend), health.getHealthService.bindService())
                        )
        // runs before the server shutdown
        _            <- ZIO.addFinalizer(ZIO.succeed(health.enterTerminalState()))
        _            <- ZIO.logInfo(s"Shard Manager server started on port ${config.apiPort}.")
        nothing      <- ZIO.never
      } yield nothing
    }

  private def buildServiceDefinition(
    shardManager: ShardManager,
    backend: ZioServerBackend[Any, StatusException, GrpcContext]
  ): ServerServiceDefinition =
    ServerService(using backend)
      .rpc(
        Protocol.getAssignments,
        (request: GetAssignmentsRequest) => shardManager.getAssignments(request.role).map(GetAssignmentsResponse(_))
      )
      .rpc(
        Protocol.register,
        (request: RegisterRequest) =>
          shardManager.register(request.pod).mapBoth(GrpcShardingService.mapErrorToStatus, _ => RegisterResponse())
      )
      .rpc(
        Protocol.unregister,
        (request: UnregisterRequest) => shardManager.unregister(request.podAddress).as(UnregisterResponse())
      )
      .rpc(
        Protocol.notifyUnhealthyPod,
        (request: NotifyUnhealthyPodRequest) =>
          shardManager.notifyUnhealthyPod(request.podAddress).as(NotifyUnhealthyPodResponse())
      )
      .rpc(
        Protocol.checkAllPodsHealth,
        (_: CheckAllPodsHealthRequest) => shardManager.checkAllPodsHealth.as(CheckAllPodsHealthResponse())
      )
      .rpc(
        Protocol.getShardingEvents,
        (_: GetShardingEventsRequest) => shardManager.getShardingEvents
      )
      .build(Protocol.service)
}
