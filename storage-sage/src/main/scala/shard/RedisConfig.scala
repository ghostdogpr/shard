package shard

/**
  * The configuration for the Redis storage implementations.
  * @param assignmentsKey a function from a role to the key to use to store shard assignments
  * @param podsKey the key to use to store registered pods
  * @param messagesKey a function from a role to the prefix of the keys to use to store `AtLeastOnce` messages and their results
  */
case class RedisConfig(
  assignmentsKey: Role => String,
  podsKey: String,
  messagesKey: Role => String = role => s"shard_messages:${role.name}"
)

object RedisConfig {
  val default: RedisConfig =
    RedisConfig(assignmentsKey = role => s"shard_assignments:${role.name}", podsKey = "pods")
}
