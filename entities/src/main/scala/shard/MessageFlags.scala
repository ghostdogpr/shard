package shard

/**
 * Marker trait for message variants processed concurrently with other messages, ignoring the entity's `concurrency`.
 */
trait Concurrent

/**
 * Marker trait for message variants that are not interrupted when their sender stops waiting (e.g. after a timeout).
 */
trait Uninterruptible
