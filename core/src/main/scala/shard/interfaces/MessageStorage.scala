package shard.interfaces

import java.util.concurrent.TimeUnit

import scala.collection.immutable.VectorMap

import zio.{Clock, Duration, Ref, Task, ULayer, ZIO, ZLayer}
import zio.stream.ZStream

import shard.{Role, ShardId}
import shard.interfaces.MessageStorage.{Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply

/**
  * An interface for storing `AtLeastOnce` messages and their results, separately for each role.
  */
trait MessageStorage {

  /**
    * Store a message before it runs, unless a message with the same key is stored, and return what was under its key.
    */
  def store(role: Role, key: Key, body: Array[Byte]): Task[Stored]

  /**
    * Save the result of a message unless its key has one, and return the result kept under its key until `retention` has passed,
    * or with no expiry for a dead letter, which keeps the body of its stored message.
    */
  def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result]

  /**
    * Remove a stored message that has no result, because it was rejected before running (e.g. its mailbox was full), or a dead letter.
    */
  def remove(role: Role, key: Key): Task[Unit]

  /**
    * Stream the stored messages of an entity type's shards that have no result, those of each entity in the order they were stored.
    * The other methods are called while the stream is open.
    */
  def pending(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, Array[Byte])]

  /**
    * Stream the dead letters of an entity type's shards, with their reason and the body of their message.
    */
  def deadLetters(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, String, Array[Byte])]
}

object MessageStorage {

  /**
    * The identity of a message: its delivery key, scoped to its entity, with the shard of its entity.
    */
  final case class Key(entityType: String, shardId: ShardId, entityId: String, deliveryKey: String)

  /**
    * The outcome of a message that won't run, with the reason.
    */
  final case class DeadLetter(reason: String)

  /**
    * The result of a message, encoded with the codecs of the message, or a dead letter: what the sends of its key get.
    */
  type Result = BinaryReply.Success | BinaryReply.Failure | DeadLetter

  /**
    * What `store` finds under a key: nothing, so the message was stored now, a message that has no result yet, or its result.
    */
  enum Stored {
    case New
    case Pending(body: Array[Byte])
    case Done(result: Result)
  }

  final private case class Completed(result: Result, expiresAt: Long, body: Array[Byte])
  private type Entry = Stored.Pending | Completed

  /**
    * A layer that stores messages in memory, which only suits a single pod, e.g. in tests.
    */
  val memory: ULayer[MessageStorage] =
    // in insertion order, which is the order of `pending`, since `store` only adds a key that is absent
    ZLayer(Ref.make(VectorMap.empty[(Role, Key), Entry]).map { entries =>
      new MessageStorage {
        private val now = Clock.currentTime(TimeUnit.MILLISECONDS)

        private def isLive(now: Long)(entry: Entry): Boolean =
          entry match {
            case Completed(_, expiresAt, _) => now < expiresAt
            case _: Stored.Pending          => true
          }

        def store(role: Role, key: Key, body: Array[Byte]): Task[Stored] =
          now.flatMap(now =>
            entries.modify { map =>
              map.get((role, key)).filter(isLive(now)) match {
                case Some(pending: Stored.Pending) => (pending, map)
                case Some(Completed(result, _, _)) => (Stored.Done(result), map)
                // the expired results are dropped here, so that they don't accumulate
                case None                          =>
                  (Stored.New, map.filter((_, entry) => isLive(now)(entry)).updated((role, key), Stored.Pending(body)))
              }
            }
          )

        def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result] =
          now.flatMap(now =>
            entries.modify(map =>
              map.get((role, key)).filter(isLive(now)) match {
                case Some(Completed(kept, _, _)) => (kept, map)
                case stored                      =>
                  val completed = (result, stored) match {
                    case (_: DeadLetter, Some(Stored.Pending(body))) => Completed(result, Long.MaxValue, body)
                    case (_: DeadLetter, _)                          => Completed(result, Long.MaxValue, Array.emptyByteArray)
                    case _                                           =>
                      Completed(result, now + retention.toMillis.min(Long.MaxValue - now), Array.emptyByteArray)
                  }
                  (result, map.updated((role, key), completed))
              }
            )
          )

        def remove(role: Role, key: Key): Task[Unit] =
          entries.update(map =>
            map.get((role, key)) match {
              case Some(_: Stored.Pending) | Some(Completed(_: DeadLetter, _, _)) => map - ((role, key))
              case _                                                              => map
            }
          )

        def pending(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, Array[Byte])] =
          ZStream.fromIterableZIO(entries.get.map(_.collect {
            case ((`role`, key), Stored.Pending(body)) if key.entityType == entityType && shards.contains(key.shardId) => (key, body)
          }))

        def deadLetters(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, String, Array[Byte])] =
          ZStream.fromIterableZIO(entries.get.map(_.collect {
            case ((`role`, key), Completed(DeadLetter(reason), _, body)) if key.entityType == entityType && shards.contains(key.shardId) =>
              (key, reason, body)
          }))
      }
    })

  /**
    * A layer that stores no messages: `AtLeastOnce` messages fail with `EntityDefect`.
    */
  val none: ULayer[MessageStorage] =
    ZLayer.succeed(new MessageStorage {
      private val noStorage = ZIO.dieMessage("MessageStorage.none stores no messages, so it can't run AtLeastOnce messages")

      def store(role: Role, key: Key, body: Array[Byte]): Task[Stored]                                                           = noStorage
      def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result]                                    = noStorage
      def remove(role: Role, key: Key): Task[Unit]                                                                               = noStorage
      def pending(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, Array[Byte])]             = ZStream.empty
      def deadLetters(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, String, Array[Byte])] = ZStream.empty
    })
}
