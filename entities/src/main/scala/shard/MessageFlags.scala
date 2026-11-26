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
trait AtMostOnce extends Uninterruptible
