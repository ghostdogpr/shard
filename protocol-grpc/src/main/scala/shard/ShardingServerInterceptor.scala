package shard

import io.grpc.StatusException
import proteus.ProtobufCodec
import proteus.server.{ GrpcContext, ServerInterceptor }
import zio._
import zio.stream.ZStream

private type UnaryEffect[A]  = IO[StatusException, A]
private type StreamEffect[A] = ZStream[Any, StatusException, A]

/**
 * Type alias for a proteus `ServerInterceptor` scoped to shard's effect types
 * (`IO` and `ZStream` with `StatusException` errors) and the default
 * `GrpcContext`. Use the smart constructors in the companion object — or implement
 * the trait directly for advanced pre/post/error wrapping.
 */
type ShardingServerInterceptor =
  ServerInterceptor[UnaryEffect, UnaryEffect, StreamEffect, StreamEffect, GrpcContext, GrpcContext]

object ShardingServerInterceptor {

  /**
   * An interceptor that does nothing — useful as a starting point or for composition.
   */
  val identity: ShardingServerInterceptor =
    ServerInterceptor.empty[UnaryEffect, StreamEffect, GrpcContext]

  /**
   * Builds an interceptor that runs `effect` before each handler. If the effect fails,
   * the RPC fails with that error and the handler is never called.
   */
  def beforeEach(effect: GrpcContext => IO[StatusException, Unit]): ShardingServerInterceptor =
    new ShardingServerInterceptor {
      def unary[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: GrpcContext => UnaryEffect[Resp]
      ): Req => GrpcContext => UnaryEffect[Resp] =
        _ => ctx => effect(ctx) *> io(ctx)

      def clientStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: StreamEffect[Req] => GrpcContext => UnaryEffect[Resp]
      ): StreamEffect[Req] => GrpcContext => UnaryEffect[Resp] =
        stream => ctx => effect(ctx) *> io(stream)(ctx)

      def serverStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: GrpcContext => StreamEffect[Resp]
      ): Req => GrpcContext => StreamEffect[Resp] =
        _ => ctx => ZStream.fromZIO(effect(ctx)) *> io(ctx)

      def bidiStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: StreamEffect[Req] => GrpcContext => StreamEffect[Resp]
      ): StreamEffect[Req] => GrpcContext => StreamEffect[Resp] =
        stream => ctx => ZStream.fromZIO(effect(ctx)) *> io(stream)(ctx)
    }

  /**
   * Composes a sequence of interceptors into a single one. The first interceptor in the
   * sequence runs outermost (i.e. its pre-logic runs first, its post-logic runs last).
   */
  def compose(interceptors: Seq[ShardingServerInterceptor]): ShardingServerInterceptor =
    interceptors.reduceRightOption(wrap).getOrElse(identity)

  private def wrap(outer: ShardingServerInterceptor, inner: ShardingServerInterceptor): ShardingServerInterceptor =
    new ShardingServerInterceptor {
      def unary[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: GrpcContext => UnaryEffect[Resp]
      ): Req => GrpcContext => UnaryEffect[Resp] =
        req => outer.unary[Req, Resp](inner.unary[Req, Resp](io).apply(req)).apply(req)

      def clientStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: StreamEffect[Req] => GrpcContext => UnaryEffect[Resp]
      ): StreamEffect[Req] => GrpcContext => UnaryEffect[Resp] =
        outer.clientStreaming[Req, Resp](inner.clientStreaming[Req, Resp](io))

      def serverStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: GrpcContext => StreamEffect[Resp]
      ): Req => GrpcContext => StreamEffect[Resp] =
        req => outer.serverStreaming[Req, Resp](inner.serverStreaming[Req, Resp](io).apply(req)).apply(req)

      def bidiStreaming[Req: ProtobufCodec, Resp: ProtobufCodec](
        io: StreamEffect[Req] => GrpcContext => StreamEffect[Resp]
      ): StreamEffect[Req] => GrpcContext => StreamEffect[Resp] =
        outer.bidiStreaming[Req, Resp](inner.bidiStreaming[Req, Resp](io))
    }
}
