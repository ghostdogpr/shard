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

  private def buildServiceDefinition(
    sharding: Sharding,
    backend: ZioServerBackend[Any, StatusException, GrpcContext]
  ): ServerServiceDefinition =
    ServerService(using backend)
      .rpc(
        Sharding.assignShards,
        (request: AssignShardsRequest) => sharding.assign(request.shards.toSet).as(AssignShardsResponse())
      )
      .rpc(
        Sharding.unassignShards,
        (request: UnassignShardsRequest) => sharding.unassign(request.shards.toSet).as(UnassignShardsResponse())
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
        _          <- serve(config.shardingPort, grpcConfig)(backend => List(buildServiceDefinition(sharding, backend)))
      } yield ()
    }
}
