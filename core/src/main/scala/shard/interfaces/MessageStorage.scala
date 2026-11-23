package shard.interfaces

import java.util.concurrent.TimeUnit

import zio.{Clock, Duration, Ref, Task, ULayer, ZIO, ZLayer}

import shard.Role
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
    * Save the result of a message unless its key has one, and return the result kept under its key until `retention` has passed.
    */
  def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result]

  /**
    * Remove a stored message that has no result, because it was rejected before running (e.g. its mailbox was full).
    */
  def remove(role: Role, key: Key): Task[Unit]
}

object MessageStorage {

  /**
    * The identity of a message: its delivery key, scoped to its entity.
    */
  final case class Key(entityType: String, entityId: String, deliveryKey: String)

  /**
    * The result of a message, encoded with the codecs of the message: the reply that the sends of its key get during its retention.
    */
  type Result = BinaryReply.Success | BinaryReply.Failure

  /**
    * What `store` finds under a key: nothing, so the message was stored now, a message that has no result yet, or its result.
    */
  enum Stored {
    case New
    case Pending(body: Array[Byte])
    case Done(result: Result)
  }

  final private case class Completed(result: Result, expiresAt: Long)
  private type Entry = Stored.Pending | Completed

  /**
    * A layer that stores messages in memory, which only suits a single pod, e.g. in tests.
    */
  val memory: ULayer[MessageStorage] =
    ZLayer(Ref.make(Map.empty[(Role, Key), Entry]).map { entries =>
      new MessageStorage {
        private val now = Clock.currentTime(TimeUnit.MILLISECONDS)

        private def isLive(now: Long)(entry: Entry): Boolean =
          entry match {
            case Completed(_, expiresAt) => now < expiresAt
            case _: Stored.Pending       => true
          }

        def store(role: Role, key: Key, body: Array[Byte]): Task[Stored] =
          now.flatMap(now =>
            entries.modify { map =>
              map.get((role, key)).filter(isLive(now)) match {
                case Some(pending: Stored.Pending) => (pending, map)
                case Some(Completed(result, _))    => (Stored.Done(result), map)
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
                case Some(Completed(kept, _)) => (kept, map)
                case _                        =>
                  (result, map.updated((role, key), Completed(result, now + retention.toMillis.min(Long.MaxValue - now))))
              }
            )
          )

        def remove(role: Role, key: Key): Task[Unit] =
          entries.update(map =>
            map.get((role, key)) match {
              case Some(_: Stored.Pending) => map - ((role, key))
              case _                       => map
            }
          )
      }
    })

  /**
    * A layer that stores no messages: `AtLeastOnce` messages fail with `EntityDefect`.
    */
  val none: ULayer[MessageStorage] =
    ZLayer.succeed(new MessageStorage {
      private val noStorage = ZIO.dieMessage("MessageStorage.none stores no messages, so it can't run AtLeastOnce messages")

      def store(role: Role, key: Key, body: Array[Byte]): Task[Stored]                        = noStorage
      def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result] = noStorage
      def remove(role: Role, key: Key): Task[Unit]                                            = noStorage
    })
}
