package shard.errors

/**
 * Exception indicating that a message was sent to a pod that is not in charge of the given entity.
 * This is expected during rebalancing and will be retried.
 */
private[shard] case class EntityNotManagedByThisPod(entityId: String)
    extends Exception(s"Entity $entityId is not managed by this pod.")
