package shard

import shard.interfaces.Storage
import com.dimafeng.testcontainers.GenericContainer
import sage.{ Endpoint, SageConfig, Topology }
import sage.backend.SageClient
import zio._
import zio.stream.ZStream
import zio.test.TestAspect.sequential
import zio.test._

object StorageRedisSpec extends ZIOSpecDefault {
  val container: ZLayer[Any, Nothing, GenericContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = new GenericContainer(dockerImage = "redis:8.8.0", exposedPorts = Seq(6379))
          container.start()
          container
        }.orDie
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }

  val redis: ZLayer[GenericContainer, Throwable, SageClient] =
    ZLayer.scoped {
      for {
        container <- ZIO.service[GenericContainer]
        endpoint   = Endpoint(container.host, container.mappedPort(container.exposedPorts.head))
        client    <- SageClient.scoped(SageConfig(topology = Topology.Standalone(endpoint)))
      } yield client
    }

  private val role = Role.default

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageRedisSpec")(
      test("save and get pods") {
        val expected =
          List(Pod(PodAddress("host1", 1), "1.0.0", role), Pod(PodAddress("host2", 2), "2.0.0", role))
            .map(p => p.address -> p)
            .toMap
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.savePods(expected))
          actual <- ZIO.serviceWithZIO[Storage](_.getPods)
        } yield assertTrue(expected == actual)
      },
      test("saving pods forgets the pods left out") {
        val kept    = Pod(PodAddress("host1", 1), "1.0.0", role)
        val removed = Pod(PodAddress("host2", 2), "2.0.0", role)
        for {
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Map(kept.address -> kept, removed.address -> removed)))
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Map(kept.address -> kept)))
          after <- ZIO.serviceWithZIO[Storage](_.getPods)
          _     <- ZIO.serviceWithZIO[Storage](_.savePods(Map.empty))
          empty <- ZIO.serviceWithZIO[Storage](_.getPods)
        } yield assertTrue(after == Map(kept.address -> kept), empty.isEmpty)
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
        val channel  = RedisConfig.default.assignmentsKey(role)
        for {
          p      <- Promise.make[Nothing, Map[Int, Option[PodAddress]]]
          _      <- ZStream.serviceWithStream[Storage](_.assignmentsStream(role)).runForeach(p.succeed(_)).fork
          // the stream only receives the assignments saved once subscribed, so wait for the subscription first
          _      <- ZIO
                      .serviceWithZIO[SageClient](_.pubsubNumSub(channel))
                      .repeatUntil(_.getOrElse(channel, 0L) > 0)
          _      <- ZIO.serviceWithZIO[Storage](_.saveAssignments(role, expected))
          actual <- p.await
        } yield assertTrue(expected == actual)
      }
    ).provideLayerShared(
      (container >>> redis) >+> (ZLayer.succeed(RedisConfig.default) >+> StorageRedis.live)
    ) @@ sequential
}
