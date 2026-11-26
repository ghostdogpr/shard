package shard

import zio.{durationInt, Duration}

/**
  * Options for an entity or topic type.
  *
  * @param maxIdleTime time without processing any message after which an entity is stopped (default: `Config.entityMaxIdleTime`)
  * @param concurrency maximum number of messages processed at the same time by an entity (default: 1, in order)
  * @param mailboxCapacity maximum number of messages waiting or being processed by an entity, beyond which messages
  *                        fail with `MailboxFull`
  * @param loadEntity whether a message should start the entity when it isn't running (otherwise `send` fails with
  *                   `MessageDiscarded`). For an `AtLeastOnce` message, it only applies to a new key.
  * @param interceptor wraps the entities of this type and the processing of their messages (see `EntityInterceptor`)
  * @param retention how long the result of an `AtLeastOnce` message is kept, during which a `send` with the same key
  *                  gets it (default: 24 hours)
  */
final case class EntityOptions[Msg[_, _]](
  maxIdleTime: Option[Duration] = None,
  concurrency: Int = 1,
  mailboxCapacity: Int = EntityOptions.DefaultMailboxCapacity,
  loadEntity: [E, A] => Msg[E, A] => Boolean = [E, A] => (_: Msg[E, A]) => true,
  interceptor: EntityInterceptor[Msg] = EntityInterceptor.none,
  retention: Duration = 24.hours
)

object EntityOptions {
  val DefaultMailboxCapacity: Int = 4096

  /**
    * Use as `mailboxCapacity` to never reject messages.
    */
  val Unbounded: Int = Int.MaxValue
}
