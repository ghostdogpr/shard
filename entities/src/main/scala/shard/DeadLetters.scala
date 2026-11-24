package shard

import zio.{RIO, Task, ZIO}
import zio.stream.ZStream

import shard.interfaces.MessageStorage

/**
  * Tools to list, retry and drop the dead letters of `AtLeastOnce` messages.
  */
object DeadLetters {

  final case class DeadLetter(entityId: String, deliveryKey: String, reason: String)

  /**
    * Stream the dead letters of an entity type from message storage, without decoding their messages.
    */
  def list(entityType: EntityType[?]): ZStream[Sharding, Throwable, DeadLetter] =
    ZStream.serviceWithStream[Sharding](sharding =>
      sharding.messageStorage
        .deadLetters(sharding.config.role, entityType.name, (1 to sharding.config.numberOfShards).toSet)
        .map((key, reason, _) => DeadLetter(key.entityId, key.deliveryKey, reason))
    )

  /**
    * Store the message of a dead letter again, so that the owner of its entity runs it at its next scan, and return false if there is none.
    */
  def retry(entityType: EntityType[?], entityId: String, deliveryKey: String): RIO[Sharding, Boolean] =
    withDeadLetter(entityType, entityId, deliveryKey) { (storage, role, key, body) =>
      // so that an interrupted retry doesn't remove the dead letter without storing its message again
      ZIO.uninterruptible(
        (storage.remove(role, key) *> storage.store(role, key, body).unit)
          .tapErrorCause(
            ZIO.logErrorCause(s"Failed to retry dead letter $deliveryKey of ${entityType.name} $entityId, whose message may no longer be stored", _)
          )
      )
    }

  /**
    * Remove a dead letter, so that the next send of its key is a new message, and return false if there is none.
    */
  def drop(entityType: EntityType[?], entityId: String, deliveryKey: String): RIO[Sharding, Boolean] =
    withDeadLetter(entityType, entityId, deliveryKey)((storage, role, key, _) => storage.remove(role, key))

  private def withDeadLetter(entityType: EntityType[?], entityId: String, deliveryKey: String)(
    use: (MessageStorage, Role, MessageStorage.Key, Array[Byte]) => Task[Unit]
  ): RIO[Sharding, Boolean] =
    ZIO.serviceWithZIO[Sharding](sharding =>
      sharding.messageStorage
        .deadLetters(sharding.config.role, entityType.name, Set(sharding.getShardId(entityType, entityId)))
        .find((key, _, _) => key.entityId == entityId && key.deliveryKey == deliveryKey)
        .runHead
        .flatMap(ZIO.foreach(_)((key, _, body) => use(sharding.messageStorage, sharding.config.role, key, body)))
        .map(_.isDefined)
    )
}
