package shard

import shard.interfaces.Pods.BinaryMessage
import shard.protocol.Sharding
import shard.protocol.Sharding._
import io.grpc._
import io.grpc.protobuf.services.ProtoReflectionServiceV1
import proteus.server.{ GrpcContext, ServerService, ZioServerBackend }
import zio.stream.ZStream
import zio.{ Config => _, _ }

import java.util.concurrent.TimeUnit

object GrpcShardingService {

  private[shard] val mapErrorToStatus: Throwable => StatusException = {
    case e: StatusException        => e
    case e: StatusRuntimeException => e.getStatus.asException()
    case e                         => Status.INTERNAL.withCause(e).withDescription(e.getMessage).asException()
  }

  // the address may have belonged to a pod of another role
  private def checkRole(role: String, expected: Role): IO[StatusException, Unit] =
    ZIO
      .fail(
        Status.FAILED_PRECONDITION
          .withDescription(s"Shards of role $role sent to a pod of role ${expected.name}")
          .asException()
      )
      .when(role != expected.name)
      .unit

  private def checkRegistration(accepted: Boolean): IO[StatusException, Unit] =
    ZIO
      .fail(Status.FAILED_PRECONDITION.withDescription("Shards sent to another registration of the pod").asException())
      .unless(accepted)
      .unit

  private def buildServiceDefinition(
    sharding: Sharding,
    role: Role,
    backend: ZioServerBackend[Any, StatusException, GrpcContext]
  ): ServerServiceDefinition =
    ServerService(using backend)
      .rpc(
        Sharding.assignShards,
        (request: AssignShardsRequest) =>
          checkRole(request.role, role) *>
            sharding
              .assign(request.registration, request.shards.toSet)
              .flatMap(checkRegistration)
              .as(AssignShardsResponse())
      )
      .rpc(
        Sharding.unassignShards,
        (request: UnassignShardsRequest) =>
          checkRole(request.role, role) *>
            sharding
              .unassign(request.registration, request.shards.toSet)
              .flatMap(checkRegistration)
              .as(UnassignShardsResponse())
      )
      .rpc(
        Sharding.send,
        (request: BinaryMessage) => sharding.sendToLocalEntity(request).mapBoth(mapErrorToStatus, SendResponse(_))
      )
      .rpc(
        Sharding.sendStream,
        (requests: ZStream[Any, StatusException, BinaryMessage]) =>
          sharding.sendStreamToLocalEntity(requests).mapBoth(mapErrorToStatus, SendResponse(_))
      )
      .rpc(
        Sharding.sendStreamAndReceiveStream,
        (requests: ZStream[Any, StatusException, BinaryMessage]) =>
          sharding.sendStreamToLocalEntityAndReceiveStream(requests).mapBoth(mapErrorToStatus, SendResponse(_))
      )
      .rpc(Sharding.pingShards, (_: PingShardsRequest) => ZIO.succeed(PingShardsResponse()))
      .build(Sharding.service)

  /**
   * Start a gRPC server on the given port with the given services, stopped when the scope ends.
   */
  private[shard] def serve(port: Int, config: GrpcConfig)(
    services: ZioServerBackend[Any, StatusException, GrpcContext] => List[ServerServiceDefinition]
  ): RIO[Scope, Unit] =
    for {
      runtime <- ZIO.runtime[Any]
      backend  = ZioServerBackend(
                   ShardingServerInterceptor.compose(config.serverInterceptors),
                   runtime,
                   config.streamingPrefetch
                 )
      _       <- ZIO.acquireRelease(ZIO.attempt {
                   val builder        = ServerBuilder.forPort(port).maxInboundMessageSize(config.maxInboundMessageSize)
                   config.executor.foreach(builder.executor)
                   services(backend).foreach(service => builder.addService(service))
                   val server: Server = builder.addService(ProtoReflectionServiceV1.newInstance()).build()
                   server.start()
                 })(server =>
                   ZIO.attemptBlocking {
                     server.shutdown()
                     server.awaitTermination(config.shutdownTimeout.toMillis, TimeUnit.MILLISECONDS)
                     server.shutdownNow()
                   }.ignore
                 )
    } yield ()

  /**
   * A layer that creates a gRPC server that exposes the Pods API.
   */
  val live: ZLayer[Config & Sharding & GrpcConfig, Throwable, Unit] =
    ZLayer.scoped[Config & Sharding & GrpcConfig] {
      for {
        config     <- ZIO.service[Config]
        grpcConfig <- ZIO.service[GrpcConfig]
        sharding   <- ZIO.service[Sharding]
        _          <- serve(config.shardingPort, grpcConfig)(backend =>
                        List(buildServiceDefinition(sharding, config.role, backend))
                      )
      } yield ()
    }
}
