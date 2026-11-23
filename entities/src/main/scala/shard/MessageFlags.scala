package shard

/**
  * Marker trait for message variants processed concurrently with other messages, ignoring the entity's `concurrency`.
  */
trait Concurrent

/**
  * Marker trait for message variants that are not interrupted when their sender stops waiting (e.g. after a timeout).
  */
trait Uninterruptible

/**
  * Marker trait for message variants that are never processed twice for one call, and not interrupted when their sender stops waiting.
  */
trait AtMostOnce extends Uninterruptible {
  final private[shard] type DeliveryGuarantee = AtMostOnce
}

/**
  * Marker trait for message variants identified by `key`, stored before they run so that every `send` with the same key
  * gets the same result, and not interrupted when their sender stops waiting.
  */
trait AtLeastOnce(key: String) extends Uninterruptible {
  final private[shard] type DeliveryGuarantee = AtLeastOnce

  // a name that the fields of a message are unlikely to use, since they can't override it
  final private[shard] def atLeastOnceKey: String = key
}
