package shard

import shard.interfaces.Storage
import zio._
import zio.test._

object StorageMemorySpec extends ZIOSpecDefault {
  private val roleA = Role("a")
  private val roleB = Role("b")
  private val pod   = PodAddress("localhost", 1234)

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageMemorySpec")(
      test("Assignments are kept separately for each role") {
        for {
          storage <- ZIO.service[Storage]
          _       <- storage.saveAssignments(roleA, Map(1 -> Some(pod)))
          _       <- storage.saveAssignments(roleB, Map(2 -> None))
          a       <- storage.getAssignments(roleA)
          b       <- storage.getAssignments(roleB)
          other   <- storage.getAssignments(Role("other"))
        } yield assertTrue(a == Map(1 -> Some(pod)), b == Map(2 -> None), other.isEmpty)
      },
      test("Assignment streams only emit the assignments of their role") {
        for {
          storage <- ZIO.service[Storage]
          _       <- storage.saveAssignments(roleA, Map(1 -> Some(pod)))
          _       <- storage.saveAssignments(roleB, Map(2 -> None))
          a       <- storage.assignmentsStream(roleA).runHead
        } yield assertTrue(a.contains(Map(1 -> Some(pod))))
      }
    ).provideLayer(Storage.memory)
}
