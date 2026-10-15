package shard.errors

import shard.PodAddress

/**
 * An error raised by the cluster rather than by the entity.
 */
sealed abstract class ClusterError(message: String) extends Exception(message)

/**
 * The mailbox of the entity is full: the message was not processed.
 */
final case class MailboxFull(entityType: String, entityId: String)
    extends ClusterError(s"Mailbox of $entityType $entityId is full")

/**
 * The call did not complete within the messenger's or broadcaster's timeout. For `send` and `broadcast`, the processing
 * of the message is interrupted (unless it is `Uninterruptible`).
 */
final case class SendTimeout(entityType: String, entityId: String)
    extends ClusterError(s"Timeout sending message to $entityType $entityId")

/**
 * A pod is not responsive. Messages to entities are retried, so only `broadcast` returns it.
 */
final case class PodUnavailable(pod: PodAddress) extends ClusterError(s"Pod $pod is unavailable")

/**
 * A stream of replies was interrupted after it started (e.g. the entity was rebalanced or its pod became unavailable).
 */
case object StreamCancelled extends ClusterError("Stream connection was canceled.")

/**
 * The shard id computed for an entity is invalid.
 */
final case class InvalidShardId(entityId: String, shardId: Int)
    extends ClusterError(s"Invalid shard id: $shardId for entity $entityId")

/**
 * The entity was stopped before the message was fully processed (e.g. it exceeded `entityTerminationTimeout`).
 */
final case class EntityInterrupted(entityType: String, entityId: String)
    extends ClusterError(s"Processing of a message by $entityType $entityId was interrupted")

/**
 * The entity was not running and `loadEntity` rejected the message.
 */
final case class MessageDiscarded(entityType: String, entityId: String)
    extends ClusterError(s"Message to $entityType $entityId was discarded because the entity is not running")

/**
 * A defect that happened while a remote entity processed a message, received as a defect (`ZIO.die`).
 */
final case class RemoteDefect(className: String, message: String)
    extends Exception(s"Remote entity died with $className: $message")
