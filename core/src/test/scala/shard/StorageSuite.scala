package shard

import shard.interfaces.Storage
import zio.Clock.ClockLive
import zio.{ durationInt, Promise, ZIO }
import zio.stream.ZStream
import zio.test._

/**
 * The tests shared by the Redis storages, run in sequence on one shared storage
 */
object StorageSuite {
  private val role = Role.default

  val tests: List[Spec[Storage, Throwable]] =
    List(
      test("save and get pods") {
        val expected = Set(
          Pod(PodAddress("host1", 1), "1.0.0", role, "r1"),
          Pod(PodAddress("host2", 2), "2.0.0", role, "r2"),
          // escaped characters, a role name with spaces and a host with colons
          Pod(PodAddress("fd00::1", 3), "3.0 %20 beta%", Role(" a b "), "id %20 x%")
        )
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.savePods(expected))
          actual <- ZIO.serviceWithZIO[Storage](_.getPods)
        } yield assertTrue(expected == actual)
      },
      test("saving pods forgets the pods left out") {
        val kept    = Pod(PodAddress("host1", 1), "1.0.0", role, "r1")
        val removed = Pod(PodAddress("host2", 2), "2.0.0", role, "r2")
        for {
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Set(kept, removed)))
          // the same effect runs twice, as a retry would run it
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Set(kept)).repeatN(1))
          after <- ZIO.serviceWithZIO[Storage](_.getPods)
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Set.empty))
          empty <- ZIO.serviceWithZIO[Storage](_.getPods)
        } yield assertTrue(after == Set(kept), empty.isEmpty)
      },
      test("save and get assignments") {
        val expected = Map(1 -> Some(PodAddress("host1", 1)), 2 -> None)
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.saveAssignments(role, expected))
          actual <- ZIO.serviceWithZIO[Storage](_.getAssignments(role))
        } yield assertTrue(expected == actual)
      },
      test("assignments stream") {
        val expected = Map(1 -> Some(PodAddress("host1", 1)), 2 -> None)
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.saveAssignments(role, expected))
          p      <- Promise.make[Nothing, Map[Int, Option[PodAddress]]]
          fiber  <- ZStream.serviceWithStream[Storage](_.assignmentsStream(role)).runForeach(p.succeed(_)).fork
          // saving no assignments keeps the saved ones but still notifies the stream. Saved again until the stream
          // receives it, since it only receives the saves made once subscribed
          _      <- (ZIO.serviceWithZIO[Storage](_.saveAssignments(role, Map.empty)) *> ClockLive.sleep(100.millis))
                      .repeatUntilZIO(_ => p.isDone)
          actual <- p.await
          _      <- fiber.interrupt
        } yield assertTrue(expected == actual)
      }
    )
}
