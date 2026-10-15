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

/**
 * The gRPC service exposed by each pod.
 */
abstract class GrpcShardingService(sharding: Sharding) {
  def assignShards(request: AssignShardsRequest): ZIO[Any, StatusException, AssignShardsResponse] =
    sharding.assign(request.shards.toSet).as(AssignShardsResponse())

  def unassignShards(request: UnassignShardsRequest): ZIO[Any, StatusException, UnassignShardsResponse] =
    sharding.unassign(request.shards.toSet).as(UnassignShardsResponse())

  def send(request: BinaryMessage): ZIO[Any, StatusException, SendResponse] =
    sharding
      .sendToLocalEntity(request)
      .map(SendResponse(_))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def sendStream(
    requests: ZStream[Any, StatusException, BinaryMessage]
  ): ZIO[Any, StatusException, SendResponse] =
    sharding
      .sendStreamToLocalEntity(requests)
      .map(SendResponse(_))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def sendStreamAndReceiveStream(
    requests: ZStream[Any, StatusException, BinaryMessage]
  ): ZStream[Any, StatusException, SendResponse] =
    sharding
      .sendStreamToLocalEntityAndReceiveStream(requests)
      .map(SendResponse(_))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def pingShards(request: PingShardsRequest): ZIO[Any, StatusException, PingShardsResponse] =
    ZIO.succeed(PingShardsResponse())
}

object GrpcShardingService {

  private[shard] val mapErrorToStatus: Throwable => StatusException = {
    case e: StatusException        => e
    case e: StatusRuntimeException => e.getStatus.asException()
    case e                         => Status.INTERNAL.withCause(e).withDescription(e.getMessage).asException()
  }

  private def buildServiceDefinition(
    service: GrpcShardingService,
    backend: ZioServerBackend[Any, StatusException, GrpcContext]
  ): ServerServiceDefinition =
    ServerService(using backend)
      .rpc(Sharding.assignShards, service.assignShards)
      .rpc(Sharding.unassignShards, service.unassignShards)
      .rpc(Sharding.send, service.send)
      .rpc(Sharding.sendStream, service.sendStream)
      .rpc(Sharding.sendStreamAndReceiveStream, service.sendStreamAndReceiveStream)
      .rpc(Sharding.pingShards, service.pingShards)
      .build(Sharding.service)

  /**
   * A layer that creates a gRPC server that exposes the Pods API.
   */
  val live: ZLayer[Config with Sharding with GrpcConfig, Throwable, Unit] =
    ZLayer.scoped[Config with Sharding with GrpcConfig] {
      for {
        config           <- ZIO.service[Config]
        grpcConfig       <- ZIO.service[GrpcConfig]
        sharding         <- ZIO.service[Sharding]
        runtime          <- ZIO.runtime[Any]
        interceptor       = ShardingServerInterceptor.compose(grpcConfig.serverInterceptors)
        backend           = ZioServerBackend(interceptor, runtime, grpcConfig.streamingPrefetch)
        service           = new GrpcShardingService(sharding) {}
        serviceDefinition = buildServiceDefinition(service, backend)
        baseBuilder       = grpcConfig.executor match {
                              case Some(executor) =>
                                ServerBuilder
                                  .forPort(config.shardingPort)
                                  .executor(executor)
                              case None           =>
                                ServerBuilder.forPort(config.shardingPort)
                            }
        builder           = baseBuilder
                              .maxInboundMessageSize(grpcConfig.maxInboundMessageSize)
                              .addService(serviceDefinition)
                              .addService(ProtoReflectionServiceV1.newInstance())
        server: Server    = builder.build()
        _                <- ZIO.acquireRelease(ZIO.attempt(server.start()))(server =>
                              ZIO.attemptBlocking {
                                server.shutdown()
                                server.awaitTermination(grpcConfig.shutdownTimeout.toMillis, TimeUnit.MILLISECONDS)
                                server.shutdownNow()
                              }.ignore
                            )
      } yield ()
    }
}
