package shard

import zio.{durationInt, Duration}

/**
  * The configuration for the Postgres message storage.
  * @param table the table that stores `AtLeastOnce` messages and their results, created with `shard/message-storage.sql`
  * @param cleanupInterval how often each pod deletes the results whose retention has passed
  * @param cleanupBatchSize how many expired results each delete removes at most
  */
case class PostgresConfig(table: String = "shard_messages", cleanupInterval: Duration = 1.minute, cleanupBatchSize: Int = 1000)

object PostgresConfig {
  val default: PostgresConfig = PostgresConfig()
}
