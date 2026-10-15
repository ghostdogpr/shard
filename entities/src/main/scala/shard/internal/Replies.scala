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
    stream.chunks
      .flatMap(
        mapChunk(_)(value => BinaryReply.Element(codec.value.encode(value)))(error =>
          ZStream.fail(serializationError("encode a stream element", error))
        )
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
    reply match {
      case BinaryReply.Success(body)                                       => decode(codec.value, body, "a reply")
      case BinaryReply.Failure(body)                                       =>
        decode(codec.error, body, "a failure").flatMap(ZIO.fail(_))
      case BinaryReply.Defect(className, msg)                              => ZIO.fail(entityDefect(className, msg))
      case BinaryReply.SerializationError(msg)                             => ZIO.fail(SerializationError(msg))
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
  ): ZStream[Any, Any, Any] = {
    def decodeOther(reply: BinaryReply): IO[Any, Any] =
      reply match {
        case BinaryReply.StreamFailure(body) =>
          decode(codec.streamError, body, "a stream failure").flatMap(ZIO.fail(_))
        case BinaryReply.Interrupted         => ZIO.fail(StreamCancelled)
        case BinaryReply.Success(_)          =>
          ZIO.dieMessage(s"Unexpected unary reply to a stream message to $entityType $entityId")
        case other                           => decodeReply(other, codec, entityType, entityId)
      }

    // decodes the elements of a chunk at once, then the reply ending the stream one by one
    def decodeChunk(chunk: Chunk[BinaryReply]): ZStream[Any, Any, Any] = {
      val elements = chunk.collectWhile { case BinaryReply.Element(body) => body }
      val rest     = chunk.drop(elements.length)
      mapChunk(elements)(codec.value.decode)(error =>
        ZStream.fail(serializationError("decode a stream element", error))
      ) ++
        (if (rest.isEmpty) ZStream.empty else ZStream.fromZIO(decodeOther(rest.head)) ++ decodeChunk(rest.drop(1)))
    }

    replies.chunks.flatMap(decodeChunk)
  }

  /**
   * Maps a whole chunk at once. Like `mapZIO`, if `f` throws, the elements before are emitted, followed by `onError`.
   */
  def mapChunk[E, A, B](chunk: Chunk[A])(f: A => B)(onError: Throwable => ZStream[Any, E, B]): ZStream[Any, E, B] = {
    val builder = ChunkBuilder.make[B](chunk.size)
    try {
      chunk.foreach(a => builder += f(a))
      ZStream.fromChunk(builder.result())
    } catch { case NonFatal(throwable) => ZStream.fromChunk(builder.result()) ++ onError(throwable) }
  }

  // `action` is what failed, e.g. "encode a reply"
  def serializationError(action: String, throwable: Throwable): SerializationError =
    SerializationError(s"Failed to $action: $throwable")

  private def entityDefect(className: String, message: String): EntityDefect =
    EntityDefect(s"Entity died with $className: $message")

  private def entityDefect(throwable: Throwable): EntityDefect =
    entityDefect(throwable.getClass.getName, String.valueOf(throwable.getMessage))

  private def decode(codec: Codec, body: Array[Byte], what: String): IO[SerializationError, Any] =
    ZIO.attempt(codec.decode(body)).mapError(serializationError(s"decode $what", _))

  private def defect(throwable: Throwable): BinaryReply =
    BinaryReply.Defect(throwable.getClass.getName, String.valueOf(throwable.getMessage))

  private def attempt(action: String)(reply: => BinaryReply): BinaryReply =
    try reply
    catch { case NonFatal(throwable) => BinaryReply.SerializationError(serializationError(action, throwable).message) }
}
