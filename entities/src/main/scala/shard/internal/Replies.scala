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

  // an interruption becomes the given error, a defect becomes EntityDefect
  def toResult(exit: Exit[Any, Any], interruptedError: => ClusterError): IO[Any, Any] =
    exit.foldExit(cause => ZIO.fail(toError(cause, interruptedError)), ZIO.succeed(_))

  def toError(cause: Cause[Any], interruptedError: => ClusterError): Any =
    cause.failureOrCause match {
      case Left(error)  => error
      case Right(other) => other.dieOption.fold[Any](interruptedError)(entityDefect)
    }

  def encodeExit(exit: Exit[Any, Any], codec: ReplyCodec): BinaryReply =
    exit match {
      case Exit.Success(value) => attempt("encode a reply")(BinaryReply.Success(codec.value.encode(value)))
      case Exit.Failure(cause) =>
        cause.failureOrCause match {
          case Left(error)  => attempt("encode a failure")(BinaryReply.Failure(codec.error.encode(error)))
          case Right(other) => other.dieOption.fold[BinaryReply](BinaryReply.Interrupted)(defect)
        }
    }

  def encodeStream(stream: ZStream[Any, Any, Any], codec: ReplyCodec): ZStream[Any, Nothing, BinaryReply] =
    stream
      .mapChunksZIO(chunk =>
        ZIO
          .attempt(chunk.map(value => BinaryReply.Element(codec.value.encode(value))))
          .mapError(serializationError("encode a stream element", _))
      )
      .catchAllCause(cause =>
        ZStream.succeed(cause.failureOrCause match {
          case Left(StreamCancelled)           => BinaryReply.Interrupted
          // the entity is stopping: the sender sends the message again
          case Left(_: Sharding.Retry)         => BinaryReply.EntityNotManagedByThisPod
          case Left(error: SerializationError) => BinaryReply.SerializationError(error.message)
          case Left(error)                     =>
            attempt("encode a stream failure")(BinaryReply.StreamFailure(codec.streamError.encode(error)))
          case Right(other)                    => other.dieOption.fold[BinaryReply](BinaryReply.Interrupted)(defect)
        })
      )

  def decodeReply(reply: BinaryReply, codec: ReplyCodec, entityType: String, entityId: String): IO[Any, Any] =
    ZIO.suspendSucceed(replyExit(reply, codec, entityType, entityId))

  def decodeStream(
    replies: ZStream[Any, Any, BinaryReply],
    codec: ReplyCodec,
    entityType: String,
    entityId: String
  ): ZStream[Any, Any, Any] = {
    // an element is decoded, a reply ending the stream becomes its failure
    def toExit(reply: BinaryReply): Exit[Option[Any], Any] =
      reply match {
        case BinaryReply.Element(body)       => Exit.succeed(codec.value.decode(body))
        case BinaryReply.StreamFailure(body) =>
          decode(codec.streamError, body, "a stream failure").flatMapExit(Exit.fail).mapErrorExit(Some(_))
        case BinaryReply.Interrupted         => Exit.fail(Some(StreamCancelled))
        case BinaryReply.Success(_)          =>
          Exit.die(new RuntimeException(s"Unexpected unary reply to a stream message to $entityType $entityId"))
        case other                           => replyExit(other, codec, entityType, entityId).mapErrorExit(Some(_))
      }

    replies
      .mapChunksZIO(chunk => ZIO.attempt(chunk.map(toExit)).mapError(serializationError("decode a stream element", _)))
      .flattenExitOption
  }

  private def replyExit(reply: BinaryReply, codec: ReplyCodec, entityType: String, entityId: String): Exit[Any, Any] =
    reply match {
      case BinaryReply.Success(body)                                       => decode(codec.value, body, "a reply")
      case BinaryReply.Failure(body)                                       =>
        decode(codec.error, body, "a failure").flatMapExit(Exit.fail)
      case BinaryReply.Defect(className, msg)                              => Exit.fail(entityDefect(className, msg))
      case BinaryReply.SerializationError(msg)                             => Exit.fail(SerializationError(msg))
      case BinaryReply.Interrupted                                         => Exit.fail(EntityInterrupted(entityType, entityId))
      case BinaryReply.MailboxFull                                         => Exit.fail(MailboxFull(entityType, entityId))
      case BinaryReply.Discarded                                           => Exit.fail(MessageDiscarded(entityType, entityId))
      case BinaryReply.EntityNotManagedByThisPod                           =>
        Exit.fail(Sharding.Retry(EntityNotManagedByThisPod(entityId)))
      case other @ (BinaryReply.Element(_) | BinaryReply.StreamFailure(_)) =>
        Exit.die(new RuntimeException(s"Unexpected stream reply $other to a unary message to $entityType $entityId"))
    }

  // `action` is what failed, e.g. "encode a reply"
  def serializationError(action: String, throwable: Throwable): SerializationError =
    SerializationError(s"Failed to $action: $throwable")

  private def entityDefect(className: String, message: String): EntityDefect =
    EntityDefect(s"Entity died with $className: $message")

  private def entityDefect(throwable: Throwable): EntityDefect =
    entityDefect(throwable.getClass.getName, String.valueOf(throwable.getMessage))

  private def decode(codec: Codec, body: Array[Byte], what: String): Exit[SerializationError, Any] =
    try Exit.succeed(codec.decode(body))
    catch { case NonFatal(throwable) => Exit.fail(serializationError(s"decode $what", throwable)) }

  private def defect(throwable: Throwable): BinaryReply =
    BinaryReply.Defect(throwable.getClass.getName, String.valueOf(throwable.getMessage))

  private def attempt(action: String)(reply: => BinaryReply): BinaryReply =
    try reply
    catch { case NonFatal(throwable) => BinaryReply.SerializationError(serializationError(action, throwable).message) }
}
