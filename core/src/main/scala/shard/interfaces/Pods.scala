package shard.interfaces

import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.{ PodAddress, ShardId }
import zio.stream.ZStream
import zio.{ Task, ULayer, ZIO, ZLayer }

/**
 * An interface to communicate with remote pods.
 * This is used by the Shard Manager for assigning and unassigning shards.
 * This is also used by pods for internal communication (forward messages to each other).
 */
trait Pods {

  /**
   * Notify a pod that it was assigned a list of shards
   */
  def assignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit]

  /**
   * Notify a pod that it was unassigned a list of shards
   */
  def unassignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit]

  /**
   * Check that a pod is responsive
   */
  def ping(pod: PodAddress): Task[Unit]

  /**
   * Send a message to a pod and receive its reply
   */
  def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply]

  /**
   * Send a stream of messages to a pod and receive a single reply once they are all enqueued, or the first rejection
   */
  def sendStream(pod: PodAddress, messages: ZStream[Any, Throwable, BinaryMessage]): Task[BinaryReply]

  /**
   * Send a stream of messages to a pod and receive a stream of replies.
   * The first message is the request, the following ones are its stream inputs, if any.
   */
  def sendStreamAndReceiveStream(
    pod: PodAddress,
    messages: ZStream[Any, Throwable, BinaryMessage]
  ): ZStream[Any, Throwable, BinaryReply]
}

object Pods {

  /**
   * A layer that creates a service that does nothing when called.
   * Useful for testing ShardManager or when we don't need messages being sent.
   */
  val noop: ULayer[Pods] =
    ZLayer.succeed(new Pods {
      def assignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit]                                  = ZIO.unit
      def unassignShards(pod: PodAddress, shards: Set[ShardId]): Task[Unit]                                = ZIO.unit
      def ping(pod: PodAddress): Task[Unit]                                                                = ZIO.unit
      def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply]                          =
        ZIO.succeed(BinaryReply.Success(Array.emptyByteArray))
      def sendStream(pod: PodAddress, messages: ZStream[Any, Throwable, BinaryMessage]): Task[BinaryReply] =
        ZIO.succeed(BinaryReply.Success(Array.emptyByteArray))
      def sendStreamAndReceiveStream(
        pod: PodAddress,
        messages: ZStream[Any, Throwable, BinaryMessage]
      ): ZStream[Any, Throwable, BinaryReply]                                                              =
        ZStream.empty
    })

  /**
   * A message sent to an entity or topic.
   * @param expectsReply if false, the receiver replies as soon as the message is enqueued
   */
  case class BinaryMessage(entityId: String, entityType: String, body: Array[Byte], expectsReply: Boolean)

  /**
   * A reply to a message. A stream of replies is a sequence of `Element`, ended by `StreamFailure`, `Defect`,
   * `SerializationError` or `Interrupted` if it fails, or a single other reply if the request itself failed or was rejected.
   */
  sealed trait BinaryReply

  object BinaryReply {

    /**
     * The message was processed and returned a value (empty when the sender didn't expect a reply)
     */
    final case class Success(body: Array[Byte]) extends BinaryReply

    /**
     * The message was processed and failed with a typed error
     */
    final case class Failure(body: Array[Byte]) extends BinaryReply

    /**
     * An element of a stream of replies
     */
    final case class Element(body: Array[Byte]) extends BinaryReply

    /**
     * A stream of replies failed with a typed error
     */
    final case class StreamFailure(body: Array[Byte]) extends BinaryReply

    /**
     * The message processing died with an unexpected error
     */
    final case class Defect(className: String, message: String) extends BinaryReply

    /**
     * The message processing was interrupted before completing (e.g. the entity was stopped)
     */
    case object Interrupted extends BinaryReply

    /**
     * The entity mailbox was full and the message was rejected
     */
    case object MailboxFull extends BinaryReply

    /**
     * The entity was not running and the message was discarded (see `loadEntity`)
     */
    case object Discarded extends BinaryReply

    /**
     * The pod doesn't host the entity (or the entity is stopping): the message was not processed and can be sent again
     */
    case object EntityNotManagedByThisPod extends BinaryReply

    /**
     * A message, reply or stream element failed to be encoded or decoded
     */
    final case class SerializationError(message: String) extends BinaryReply
  }
}
