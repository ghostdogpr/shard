package shard

import shard.errors.{ ClusterError, StreamCancelled }
import zio._
import zio.stream.ZStream

import scala.annotation.implicitNotFound
import scala.util.NotGiven

/**
 * An interface to communicate with a remote entity.
 * The messenger's timeout only applies to `send` and `sendDiscard`.
 * @tparam Msg the type of message that can be sent to this entity type
 */
trait Messenger[Msg[+_, +_]] {

  /**
   * Send a message and wait for its reply.
   * The message may be processed more than once if its pod becomes unavailable.
   */
  def send[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[E | ClusterError, A]

  /**
   * Send a message without waiting for its reply.
   * Returns once the message is in the entity's mailbox.
   */
  def sendDiscard[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[ClusterError, Unit]

  /**
   * Send a message and receive a stream of replies.
   * The stream fails with `StreamCancelled` if the entity is rebalanced (see `streamAutoRestart`).
   */
  def stream[E, E2, A](entityId: String)(message: Msg[E, Streamed[E2, A]]): ZStream[Any, E | E2 | ClusterError, A]

  /**
   * Send a message with a stream of inputs and receive a stream of replies.
   * The stream fails with `StreamCancelled` if the entity is rebalanced (see `duplexAutoRestart`).
   */
  def duplex[E, In, E1, E2, A](entityId: String)(
    message: Msg[E, Duplex[In, E2, A]],
    input: ZStream[Any, E1, In]
  ): ZStream[Any, E | E1 | E2 | ClusterError, A]

  /**
   * Send a stream of messages in order, without waiting for their replies.
   * Returns once they are all in the entity's mailbox, or fails at the first rejected message.
   */
  def sendStream[E1](entityId: String)(messages: ZStream[Any, E1, Msg[Nothing, Unit]]): IO[E1 | ClusterError, Unit]

  /**
   * Send a message and receive a stream of replies, restarting it when the entity is rebalanced.
   * The message is built from a cursor that is updated with each reply, so the stream restarts where it ended.
   */
  def streamAutoRestart[Cursor, E, E2, A](entityId: String, cursor: Cursor)(
    message: Cursor => Msg[E, Streamed[E2, A]]
  )(updateCursor: (Cursor, A) => Cursor): ZStream[Any, E | E2 | ClusterError, A] =
    Messenger.autoRestart[Cursor, E | E2 | ClusterError, A](cursor)(c => stream(entityId)(message(c)))(updateCursor)

  /**
   * Send a message with a stream of inputs and receive a stream of replies, restarting it when the entity is rebalanced.
   * The message and inputs are built from a cursor, as in `streamAutoRestart`.
   */
  def duplexAutoRestart[Cursor, E, In, E1, E2, A](entityId: String, cursor: Cursor)(
    message: Cursor => Msg[E, Duplex[In, E2, A]],
    input: Cursor => ZStream[Any, E1, In]
  )(updateCursor: (Cursor, A) => Cursor): ZStream[Any, E | E1 | E2 | ClusterError, A] =
    Messenger.autoRestart[Cursor, E | E1 | E2 | ClusterError, A](cursor)(c => duplex(entityId)(message(c), input(c)))(
      updateCursor
    )
}

object Messenger {
  sealed trait MessengerTimeout
  object MessengerTimeout {
    case object NoTimeout                  extends MessengerTimeout
    case object InheritConfigTimeout       extends MessengerTimeout
    case class Timeout(duration: Duration) extends MessengerTimeout
  }

  private[shard] def autoRestart[Cursor, E, A](cursor: Cursor)(
    stream: Cursor => ZStream[Any, E, A]
  )(updateCursor: (Cursor, A) => Cursor): ZStream[Any, E, A] =
    ZStream.unwrap(Ref.make(cursor).map { lastSeen =>
      def run: ZStream[Any, E, A] =
        ZStream
          .unwrap(lastSeen.get.map(stream))
          .mapChunksZIO(chunk => lastSeen.update(chunk.foldLeft(_)(updateCursor)).as(chunk))
          .catchSome { case StreamCancelled => ZStream.execute(ZIO.sleep(200.millis)) ++ run }
      run
    })
}

/**
 * Evidence that a reply is a single value, not a stream.
 */
@implicitNotFound(
  "Cannot send a message whose reply type is ${A} with `send` or `sendDiscard`. Use `stream` for Streamed replies and `duplex` for Duplex replies. A reply type that could be a stream (e.g. Any) or Nothing is not supported: use Unit for messages that reply with nothing."
)
sealed trait Unary[A]

object Unary {
  private val instance: Unary[Any] = new Unary[Any] {}

  given [A](using
    NotGiven[A <:< Streamed[?, ?]],
    NotGiven[A <:< Duplex[?, ?, ?]],
    NotGiven[Streamed[Nothing, Nothing] <:< A],
    NotGiven[Duplex[Any, Nothing, Nothing] <:< A]
  ): Unary[A] =
    instance.asInstanceOf[Unary[A]]
}
