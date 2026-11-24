package shard

import java.nio.charset.StandardCharsets.UTF_8

import zio.{durationInt, Clock, Duration, ZIO}
import zio.test._

import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{DeadLetter, Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply

/**
  * The tests shared by the message storages, run in sequence on one storage with a live clock, each with its own keys.
  */
object MessageStorageSuite {
  private val role = Role.default

  private def store(key: Key, body: String): ZIO[MessageStorage, Throwable, String] =
    ZIO.serviceWithZIO[MessageStorage](_.store(role, key, body.getBytes(UTF_8)).map(render))

  private def saveResult(key: Key, result: Result, retention: Duration = 1.minute): ZIO[MessageStorage, Throwable, String] =
    ZIO.serviceWithZIO[MessageStorage](_.saveResult(role, key, result, retention)).map(result => render(Stored.Done(result)))

  private def remove(key: Key): ZIO[MessageStorage, Throwable, Unit] =
    ZIO.serviceWithZIO[MessageStorage](_.remove(role, key))

  private def success(body: String): Result = BinaryReply.Success(body.getBytes(UTF_8))
  private def failure(body: String): Result = BinaryReply.Failure(body.getBytes(UTF_8))

  // byte arrays are compared by reference
  private def render(stored: Stored): String =
    stored match {
      case Stored.New                             => "new"
      case Stored.Pending(body)                   => s"pending ${String(body, UTF_8)}"
      case Stored.Done(BinaryReply.Success(body)) => s"success ${String(body, UTF_8)}"
      case Stored.Done(BinaryReply.Failure(body)) => s"failure ${String(body, UTF_8)}"
      case Stored.Done(DeadLetter(reason))        => s"dead letter $reason"
    }

  val tests: List[Spec[MessageStorage, Throwable]] =
    List(
      test("a stored message is pending until its result is saved, and only its first result is kept") {
        val key = Key("type", 1, "id", "pending")
        for {
          first   <- store(key, "a")
          pending <- store(key, "b")
          saved   <- saveResult(key, failure("error"))
          kept    <- saveResult(key, success("value"))
          done    <- store(key, "c")
        } yield assertTrue(
          first == "new",
          pending == "pending a",
          saved == "failure error",
          kept == "failure error",
          done == "failure error"
        )
      },
      test("keys are separate for each role, entity type and entity ID") {
        val key = Key("type", 1, "id", "scoped")
        for {
          _      <- store(key, "a")
          others <- ZIO.foreach(List(key.copy(entityType = "other"), key.copy(entityId = "other")))(store(_, "b"))
          role   <- ZIO.serviceWithZIO[MessageStorage](_.store(Role("other"), key, "c".getBytes(UTF_8)))
        } yield assertTrue(others.forall(_ == "new"), role == Stored.New)
      },
      test("a result is kept for its retention, then the key is free") {
        val key = Key("type", 1, "id", "retention")
        for {
          _     <- store(key, "a")
          _     <- saveResult(key, success("value"), retention = 500.millis)
          kept  <- store(key, "b")
          _     <- (Clock.sleep(100.millis) *> store(key, "c")).repeatUntil(_ == "new")
          again <- store(key, "d")
        } yield assertTrue(kept == "success value", again == "pending c")
      },
      test("a dead letter is kept with no expiry, and like a result, only the first outcome is kept") {
        val deadLetter = Key("type", 1, "id", "dead letter")
        val done       = Key("type", 1, "id", "done before dead letter")
        for {
          _      <- store(deadLetter, "a")
          saved  <- saveResult(deadLetter, DeadLetter("reason"), retention = 1.milli)
          kept   <- saveResult(deadLetter, success("value"))
          _      <- remove(deadLetter)
          _      <- Clock.sleep(100.millis)
          stored <- store(deadLetter, "b")
          _      <- store(done, "a")
          _      <- saveResult(done, success("value"))
          result <- saveResult(done, DeadLetter("reason"))
        } yield assertTrue(
          saved == "dead letter reason",
          kept == "dead letter reason",
          stored == "dead letter reason",
          result == "success value"
        )
      },
      test("removing a message frees its key, unless it has a result") {
        val removed = Key("type", 1, "id", "removed")
        val done    = Key("type", 1, "id", "done")
        for {
          _       <- store(removed, "a")
          _       <- remove(removed)
          stored  <- store(removed, "b")
          _       <- store(done, "a")
          _       <- saveResult(done, success("value"))
          _       <- remove(done)
          result  <- store(done, "b")
          _       <- saveResult(Key("type", 1, "id", "unknown"), success("value"))
          unknown <- store(Key("type", 1, "id", "unknown"), "a")
        } yield assertTrue(stored == "new", result == "success value", unknown == "success value")
      },
      test("pending streams the messages of an entity type's shards that have no result") {
        def key(shardId: ShardId, deliveryKey: String) = Key("pending-type", shardId, "id", deliveryKey)
        for {
          _       <-
            ZIO.foreachDiscard(List(key(1, "a"), key(2, "b"), key(3, "other shard"), key(1, "done"), key(1, "dead letter"), key(1, "removed")))(key =>
              store(key, key.deliveryKey)
            )
          _       <- saveResult(key(1, "done"), success("value"))
          _       <- saveResult(key(1, "dead letter"), DeadLetter("reason"))
          _       <- remove(key(1, "removed"))
          _       <- store(Key("other", 1, "id", "a"), "c")
          _       <- ZIO.serviceWithZIO[MessageStorage](_.store(Role("other"), key(1, "other role"), "d".getBytes(UTF_8)))
          pending <- ZIO.serviceWithZIO[MessageStorage](_.pending(role, "pending-type", Set(1, 2)).runCollect)
        } yield assertTrue(pending.map((key, body) => key -> String(body, UTF_8)).toSet == Set(key(1, "a") -> "a", key(2, "b") -> "b"))
      }
    )
}
