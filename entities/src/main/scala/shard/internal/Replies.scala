package shard.internal

import shard.Sharding
import shard.errors._
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }
import shard.interfaces.Pods.BinaryReply
import zio._
import zio.stream.ZStream

import scala.util.control.NonFatal

/**
 * Conversions between the results of messages and their binary replies.
 */
private[shard] object Replies {

  // an interruption becomes the given error
  def toResult(exit: Exit[Any, Any], interruptedError: => ClusterError): IO[Any, Any] =
    exit.foldExit(
      cause => if (cause.isInterruptedOnly) ZIO.fail(interruptedError) else ZIO.failCause(cause),
      ZIO.succeed(_)
    )

  def encodeExit(exit: Exit[Any, Any], codec: ReplyCodec): BinaryReply =
    exit match {
      case Exit.Success(value) => attempt(BinaryReply.Success(codec.value.encode(value)))
      case Exit.Failure(cause) =>
        cause.failureOrCause match {
          case Left(error)  => attempt(BinaryReply.Failure(codec.error.encode(error)))
          case Right(other) => other.dieOption.fold[BinaryReply](BinaryReply.Interrupted)(defect)
        }
    }

  def encodeStream(stream: ZStream[Any, Any, Any], codec: ReplyCodec): ZStream[Any, Nothing, BinaryReply] =
    stream
      .mapZIO(value => ZIO.attempt(BinaryReply.Element(codec.value.encode(value))).orDie)
      .catchAllCause(cause =>
        ZStream.succeed(cause.failureOrCause match {
          case Left(StreamCancelled)   => BinaryReply.Interrupted
          // the entity is stopping: the sender sends the message again
          case Left(_: Sharding.Retry) => BinaryReply.EntityNotManagedByThisPod
          case Left(error)             => attempt(BinaryReply.StreamFailure(codec.streamError.encode(error)))
          case Right(other)            => other.dieOption.fold[BinaryReply](BinaryReply.Interrupted)(defect)
        })
      )

  def decodeReply(reply: BinaryReply, codec: ReplyCodec, entityType: String, entityId: String): IO[Any, Any] =
    reply match {
      case BinaryReply.Success(body)                                       => decode(codec.value, body)
      case BinaryReply.Failure(body)                                       => decode(codec.error, body).flip
      case BinaryReply.Defect(className, msg)                              => ZIO.die(RemoteDefect(className, msg))
      case BinaryReply.Interrupted                                         => ZIO.fail(EntityInterrupted(entityType, entityId))
      case BinaryReply.MailboxFull                                         => ZIO.fail(MailboxFull(entityType, entityId))
      case BinaryReply.Discarded                                           => ZIO.fail(MessageDiscarded(entityType, entityId))
      case BinaryReply.EntityNotManagedByThisPod                           =>
        ZIO.fail(Sharding.Retry(EntityNotManagedByThisPod(entityId)))
      case other @ (BinaryReply.Element(_) | BinaryReply.StreamFailure(_)) =>
        ZIO.dieMessage(s"Unexpected stream reply $other to a unary message to $entityType $entityId")
    }

  def decodeStream(
    replies: ZStream[Any, Any, BinaryReply],
    codec: ReplyCodec,
    entityType: String,
    entityId: String
  ): ZStream[Any, Any, Any] =
    replies.mapZIO {
      case BinaryReply.Element(body)       => decode(codec.value, body)
      case BinaryReply.StreamFailure(body) => decode(codec.streamError, body).flatMap(ZIO.fail(_))
      case BinaryReply.Interrupted         => ZIO.fail(StreamCancelled)
      case BinaryReply.Success(_)          =>
        ZIO.dieMessage(s"Unexpected unary reply to a stream message to $entityType $entityId")
      case other                           =>
        decodeReply(other, codec, entityType, entityId)
    }

  private def decode(codec: Codec, body: Array[Byte]): UIO[Any] =
    ZIO.attempt(codec.decode(body)).orDie

  private def defect(throwable: Throwable): BinaryReply =
    throwable match {
      case RemoteDefect(className, message) => BinaryReply.Defect(className, message)
      case other                            => BinaryReply.Defect(other.getClass.getName, String.valueOf(other.getMessage))
    }

  private def attempt(reply: => BinaryReply): BinaryReply =
    try reply
    catch { case NonFatal(throwable) => defect(throwable) }
}
