package shard

import io.grpc.ClientInterceptor
import zio._

import java.util.concurrent.Executor

/**
 * The configuration for the gRPC client and server.
 *
 * @param maxInboundMessageSize the maximum message size allowed to be received by the grpc client and server
 * @param executor a custom executor to pass to grpc-java when creating gRPC clients and servers
 * @param shutdownTimeout the timeout to wait for the gRPC server to shutdown before forcefully shutting it down
 * @param streamingPrefetch the in-flight window for streaming RPCs (request/response messages fetched ahead of the consumer)
 * @param clientInterceptors the interceptors to be used by the gRPC client, e.g. for adding tracing or logging
 * @param serverInterceptors the interceptors to be used by the gRPC server, e.g. for adding tracing or logging
 * @param connectionIdleTimeout how long a connection to another pod can stay unused before it is closed (e.g. after that pod left)
 */
case class GrpcConfig(
  maxInboundMessageSize: Int,
  executor: Option[Executor],
  shutdownTimeout: Duration,
  streamingPrefetch: Int,
  clientInterceptors: Seq[ClientInterceptor],
  serverInterceptors: Seq[ShardingServerInterceptor],
  connectionIdleTimeout: Duration
)

object GrpcConfig {
  val default: GrpcConfig =
    GrpcConfig(
      maxInboundMessageSize = 32 * 1024 * 1024,
      executor = None,
      shutdownTimeout = 3.seconds,
      streamingPrefetch = 16,
      clientInterceptors = Seq.empty,
      serverInterceptors = Seq.empty,
      connectionIdleTimeout = 5.minutes
    )
}
