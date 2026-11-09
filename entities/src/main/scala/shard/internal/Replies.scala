package shard.internal

import shard.Sharding
import shard.errors._
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }
import shard.interfaces.Pods.BinaryReply
import zio._
import zio.stream.ZStream

/**
 * Conversions between the results of messages and their binary replies.
 */
private[shard] object Replies {

  /**
   * Convert the result of a message to the result seen by its sender.
   * An interruption becomes `interruptedError` and a defect becomes `EntityDefect`.
   */
  def toResult(exit: Exit[Any, Any], interruptedError: => ClusterError): Exit[Any, Any] =
    exit.foldExit(cause => Exit.fail(toError(cause, interruptedError)), Exit.succeed)

  def toError(cause: Cause[Any], interruptedError: => ClusterError): Any =
    foldCause[Any](cause, entityDefect, interruptedError)(identity)

  def encodeExit(exit: Exit[Any, Any], codec: ReplyCodec): BinaryReply =
    exit match {
      case Exit.Success(value) => attempt("encode a reply")(BinaryReply.Success(codec.value.encode(value)))
      case Exit.Failure(cause) =>
        foldCause[BinaryReply](cause, defect, BinaryReply.Interrupted)(error =>
          attempt("encode a failure")(BinaryReply.Failure(codec.error.encode(error)))
        )
    }

  def encodeStream(stream: ZStream[Any, Any, Any], codec: ReplyCodec): ZStream[Any, Nothing, BinaryReply] =
    mapElements(stream, "encode a stream element")(value => BinaryReply.Element(codec.value.encode(value)))
      .catchAllCause(cause =>
        ZStream.succeed(foldCause[BinaryReply](cause, defect, BinaryReply.Interrupted) {
          case StreamCancelled           => BinaryReply.Interrupted
          // the entity is stopping: the sender sends the message again
          case _: Sharding.Retry         => BinaryReply.EntityNotManagedByThisPod
          case error: SerializationError => BinaryReply.SerializationError(error.message)
          case error                     =>
            attempt("encode a stream failure")(BinaryReply.StreamFailure(codec.streamError.encode(error)))
        })
      )

  def decodeStream(
    replies: ZStream[Any, Any, BinaryReply],
    codec: ReplyCodec,
    entityType: String,
    entityId: String
  ): ZStream[Any, Any, Any] = {
    // an element is decoded, a reply ending the stream becomes its failure
    def toExit(reply: BinaryReply): Exit[Option[Any], Any] =
      reply match {
        case BinaryReply.Element(body)       => decode(codec.value, body, "a stream element").mapErrorExit(Some(_))
        case BinaryReply.StreamFailure(body) =>
          decode(codec.streamError, body, "a stream failure").flatMapExit(Exit.fail).mapErrorExit(Some(_))
        case BinaryReply.Interrupted         => Exit.fail(Some(StreamCancelled))
        case BinaryReply.Success(_)          =>
          Exit.die(new RuntimeException(s"Unexpected unary reply to a stream message to $entityType $entityId"))
        case other                           => decodeReply(other, codec, entityType, entityId).mapErrorExit(Some(_))
      }

    replies.map(toExit).flattenExitOption
  }

  def decodeReply(reply: BinaryReply, codec: ReplyCodec, entityType: String, entityId: String): Exit[Any, Any] =
    reply match {
      case BinaryReply.Success(body)                                       => decode(codec.value, body, "a reply")
      case BinaryReply.Failure(body)                                       =>
        decode(codec.error, body, "a failure").flatMapExit(Exit.fail)
      case BinaryReply.Defect(msg)                                         => Exit.fail(EntityDefect(msg))
      case BinaryReply.SerializationError(msg)                             => Exit.fail(SerializationError(msg))
      case BinaryReply.Interrupted                                         => Exit.fail(EntityInterrupted(entityType, entityId))
      case BinaryReply.MailboxFull                                         => Exit.fail(MailboxFull(entityType, entityId))
      case BinaryReply.Discarded                                           => Exit.fail(MessageDiscarded(entityType, entityId))
      case BinaryReply.EntityNotManagedByThisPod                           =>
        Exit.fail(Sharding.Retry(EntityNotManagedByThisPod(entityId)))
      case other @ (BinaryReply.Element(_) | BinaryReply.StreamFailure(_)) =>
        Exit.die(new RuntimeException(s"Unexpected stream reply $other to a unary message to $entityType $entityId"))
    }

  /**
   * Create a `SerializationError` for a failed `action`, such as "encode a reply".
   */
  def serializationError(action: String, throwable: Throwable): SerializationError =
    SerializationError(s"Failed to $action: $throwable")

  /**
   * Convert the elements of a stream with a codec, failing with a `SerializationError` for `action` if it throws.
   */
  def mapElements[E, A, B](stream: ZStream[Any, E, A], action: String)(
    f: A => B
  ): ZStream[Any, E | SerializationError, B] =
    stream.mapChunksZIO(chunk =>
      catching[Exit[SerializationError, Chunk[B]]](action)(Exit.succeed(chunk.map(f)))(Exit.fail)
    )

  // what `ZIO.attempt` catches, without its work per call
  private inline def catching[A](inline action: String)(inline body: A)(inline onError: SerializationError => A): A =
    try body
    catch {
      case throwable if !throwable.isInstanceOf[VirtualMachineError] => onError(serializationError(action, throwable))
    }

  // a defect wins over a failure in the same cause, since it stops the entity
  private def foldCause[B](cause: Cause[Any], onDefect: Throwable => B, onInterrupt: => B)(onFailure: Any => B): B =
    cause.dieOption.fold(cause.failureOption.fold(onInterrupt)(onFailure))(onDefect)

  private def entityDefect(throwable: Throwable): EntityDefect =
    EntityDefect(s"Entity died with ${throwable.getClass.getName}: ${throwable.getMessage}")

  private def decode(codec: Codec, body: Array[Byte], what: String): Exit[SerializationError, Any] =
    catching[Exit[SerializationError, Any]](s"decode $what")(Exit.succeed(codec.decode(body)))(Exit.fail)

  private def defect(throwable: Throwable): BinaryReply =
    BinaryReply.Defect(entityDefect(throwable).message)

  private def attempt(action: String)(reply: => BinaryReply): BinaryReply =
    catching(action)(reply)(error => BinaryReply.SerializationError(error.message))
}
