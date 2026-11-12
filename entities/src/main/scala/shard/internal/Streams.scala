package shard.internal

import zio._
import zio.stream.{ ZChannel, ZSink, ZStream }

/**
 * ZIO logs every fiber that fails as an unhandled error, even when its failure is handled, and runs a stream, or each
 * side of a merged stream, in its own fiber: these operators keep failures out of those fibers.
 */
private[shard] object Streams {

  /**
   * Like `stream.interruptWhen(io)`.
   */
  def interruptWhen[E, A](stream: ZStream[Any, E, A], io: IO[E, Any]): ZStream[Any, E, A] =
    ZStream.fromChannel(
      stream.channel
        .foldCauseChannel(cause => ZChannel.succeed(Some(cause)), _ => ZChannel.succeed(None))
        .interruptWhen(io.foldCause(Some(_), _ => None))
        .flatMap(_.fold(ZChannel.unit)(ZChannel.refailCause))
    )

  /**
   * Like `stream.run(sink)`, though a failure of the sink is still logged.
   */
  def run[E, A, Z](stream: ZStream[Any, E, A], sink: ZSink[Any, E, A, Any, Z]): IO[E, Z] =
    Promise
      .make[E, Nothing]
      .flatMap(failure => failInto(stream, failure).run(sink) <* ZIO.whenZIODiscard(failure.isDone)(failure.await))

  /**
   * End the stream instead of failing, and fail `failure` with its cause.
   * When the stream fails and its finalizer dies, the cause is the defect, as a defect wins over a failure.
   */
  def failInto[E, A](stream: ZStream[Any, E, A], failure: Promise[E, ?]): ZStream[Any, Nothing, A] =
    ZStream
      .unwrapScoped(stream.toPull.map(ZStream.repeatZIOChunkOption(_)))
      .catchAllCause(cause => ZStream.execute(failure.failCause(cause)))
}
