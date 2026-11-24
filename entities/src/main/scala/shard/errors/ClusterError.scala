package shard.errors

import shard.PodAddress

/**
  * An error raised by the cluster rather than by the entity.
  */
sealed abstract class ClusterError(message: String) extends Exception(message)

/**
  * The mailbox of the entity is full: the message was not processed.
  */
final case class MailboxFull(entityType: String, entityId: String) extends ClusterError(s"Mailbox of $entityType $entityId is full")

/**
  * The call did not complete within the messenger's or broadcaster's timeout.
  */
final case class SendTimeout(entityType: String, entityId: String) extends ClusterError(s"Timeout sending message to $entityType $entityId")

/**
  * A pod is not responsive. Messages to entities are retried, so only `broadcast` returns it.
  */
final case class PodUnavailable(pod: PodAddress) extends ClusterError(s"Pod $pod is unavailable")

/**
  * A stream of replies was interrupted after it started (e.g. the entity was rebalanced or its pod became unavailable),
  * or before that for an `AtMostOnce` message (e.g. its entity stopped).
  */
case object StreamCancelled extends ClusterError("Stream connection was canceled.")

/**
  * The shard id computed for an entity is invalid.
  */
final case class InvalidShardId(entityId: String, shardId: Int) extends ClusterError(s"Invalid shard id: $shardId for entity $entityId")

/**
  * The entity was stopped before the message was fully processed (e.g. it exceeded `entityTerminationTimeout`).
  */
final case class EntityInterrupted(entityType: String, entityId: String)
  extends ClusterError(s"Processing of a message by $entityType $entityId was interrupted")

/**
  * An `AtMostOnce` message may or may not have been processed (e.g. its pod became unavailable after it was sent),
  * so it was not sent again.
  */
final case class DeliveryUnknown(entityType: String, entityId: String)
  extends ClusterError(s"Message to $entityType $entityId may or may not have been processed")

/**
  * Message storage can't be reached, so the outcome of an `AtLeastOnce` message is unknown. It may have been processed:
  * sending it again with the same key gets its result if that was stored, and processes it again otherwise.
  */
final case class StorageUnavailable(entityType: String, entityId: String)
  extends ClusterError(s"Message storage is unavailable for a message to $entityType $entityId")

/**
  * An `AtLeastOnce` message became a dead letter, so it won't run again: its sends with the same key get this error.
  */
final case class DeadLettered(entityType: String, entityId: String, reason: String)
  extends ClusterError(s"Message to $entityType $entityId became a dead letter: $reason")

/**
  * The entity was not running and `loadEntity` rejected the message.
  */
final case class MessageDiscarded(entityType: String, entityId: String)
  extends ClusterError(s"Message to $entityType $entityId was discarded because the entity is not running")

/**
  * A message, reply or stream element failed to be encoded or decoded.
  */
final case class SerializationError(message: String) extends ClusterError(message)

/**
  * An unexpected failure of the transport between pods.
  */
final case class TransportError(message: String) extends ClusterError(message)

/**
  * The entity died while processing the message or streaming its reply (or failed to start).
  */
final case class EntityDefect(message: String) extends ClusterError(message)
