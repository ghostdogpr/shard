package shard

import java.nio.charset.StandardCharsets.UTF_8

import zio.{durationInt, Clock, Duration, ZIO}
import zio.test._

import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{Key, Result, Stored}
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
    }

  val tests: List[Spec[MessageStorage, Throwable]] =
    List(
      test("a stored message is pending until its result is saved, and only its first result is kept") {
        val key = Key("type", "id", "pending")
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
        val key = Key("type", "id", "scoped")
        for {
          _      <- store(key, "a")
          others <- ZIO.foreach(List(key.copy(entityType = "other"), key.copy(entityId = "other")))(store(_, "b"))
          role   <- ZIO.serviceWithZIO[MessageStorage](_.store(Role("other"), key, "c".getBytes(UTF_8)))
        } yield assertTrue(others.forall(_ == "new"), role == Stored.New)
      },
      test("a result is kept for its retention, then the key is free") {
        val key = Key("type", "id", "retention")
        for {
          _     <- store(key, "a")
          _     <- saveResult(key, success("value"), retention = 500.millis)
          kept  <- store(key, "b")
          _     <- (Clock.sleep(100.millis) *> store(key, "c")).repeatUntil(_ == "new")
          again <- store(key, "d")
        } yield assertTrue(kept == "success value", again == "pending c")
      },
      test("removing a message frees its key, unless it has a result") {
        val removed = Key("type", "id", "removed")
        val done    = Key("type", "id", "done")
        for {
          _       <- store(removed, "a")
          _       <- remove(removed)
          stored  <- store(removed, "b")
          _       <- store(done, "a")
          _       <- saveResult(done, success("value"))
          _       <- remove(done)
          result  <- store(done, "b")
          _       <- saveResult(Key("type", "id", "unknown"), success("value"))
          unknown <- store(Key("type", "id", "unknown"), "a")
        } yield assertTrue(stored == "new", result == "success value", unknown == "success value")
      }
    )
}
