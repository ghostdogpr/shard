package shard

import shard.errors.EntityNotManagedByThisPod
import shard.interfaces.Pods.BinaryMessage
import shard.protocol.Sharding
import shard.protocol.Sharding._
import io.grpc._
import io.grpc.protobuf.services.ProtoReflectionServiceV1
import proteus.server.{ GrpcContext, ServerService, ZioServerBackend }
import zio.stream.ZStream
import zio.{ Config => _, _ }

import java.util.concurrent.TimeUnit

abstract class GrpcShardingService(sharding: Sharding, timeout: Duration) {
  def assignShards(request: AssignShardsRequest): ZIO[Any, StatusException, AssignShardsResponse] =
    sharding.assign(request.shards.toSet).as(AssignShardsResponse())

  def unassignShards(request: UnassignShardsRequest): ZIO[Any, StatusException, UnassignShardsResponse] =
    sharding.unassign(request.shards.toSet).as(UnassignShardsResponse())

  def send(request: SendRequest): ZIO[Any, StatusException, SendResponse] =
    sharding
      .sendToLocalEntity(GrpcShardingService.toBinary(request))
      .map(SendResponse(_))
      .mapError(GrpcShardingService.mapErrorToStatus)
      .timeoutFail(GrpcShardingService.timeoutException)(timeout)

  def sendStream(
    requests: ZStream[Any, StatusException, SendRequest]
  ): ZIO[Any, StatusException, SendResponse] =
    sharding
      .sendStreamToLocalEntity(requests.map(GrpcShardingService.toBinary))
      .map(SendResponse(_))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def sendAndReceiveStream(request: SendRequest): ZStream[Any, StatusException, SendResponse] =
    sharding
      .sendToLocalEntityAndReceiveStream(GrpcShardingService.toBinary(request))
      .map(bytes => SendResponse(Some(bytes)))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def sendStreamAndReceiveStream(
    requests: ZStream[Any, StatusException, SendRequest]
  ): ZStream[Any, StatusException, SendResponse] =
    sharding
      .sendStreamToLocalEntityAndReceiveStream(requests.map(GrpcShardingService.toBinary))
      .map(bytes => SendResponse(Some(bytes)))
      .mapError(GrpcShardingService.mapErrorToStatus)

  def pingShards(request: PingShardsRequest): ZIO[Any, StatusException, PingShardsResponse] =
    ZIO.succeed(PingShardsResponse())
}

object GrpcShardingService {

  private[shard] val timeoutException: StatusException =
    Status.ABORTED.withDescription("Timeout while handling sharding send grpc").asException()

  private[shard] def toBinary(req: SendRequest): BinaryMessage =
    BinaryMessage(req.entityId, req.entityType, req.body, req.replyId)

  private[shard] val mapErrorToStatus: Throwable => StatusException = {
    case e: StatusException           => e
    case e: StatusRuntimeException    => e.getStatus.asException()
    case e: EntityNotManagedByThisPod => Status.RESOURCE_EXHAUSTED.withCause(e).asException()
    case e                            => Status.INTERNAL.withCause(e).withDescription(e.getMessage).asException()
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
      .rpc(Sharding.sendAndReceiveStream, service.sendAndReceiveStream)
      .rpc(Sharding.sendStreamAndReceiveStream, service.sendStreamAndReceiveStream)
      .rpc(Sharding.pingShards, service.pingShards)
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
        service     = new GrpcShardingService(sharding, config.sendTimeout) {}
        _          <- serve(config.shardingPort, grpcConfig)(backend => List(buildServiceDefinition(service, backend)))
      } yield ()
    }
}
