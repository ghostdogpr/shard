package shard

import shard.interfaces.Storage
import com.dimafeng.testcontainers.GenericContainer
import org.redisson.Redisson
import org.redisson.config.{ Config => RedissonConfig }
import org.redisson.api.RedissonClient
import zio.Clock.ClockLive
import zio._
import zio.stream.ZStream
import zio.test.TestAspect.sequential
import zio.test._

object StorageRedisSpec extends ZIOSpecDefault {
  val container: ZLayer[Any, Nothing, GenericContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = new GenericContainer(dockerImage = "redis:6.2.5", exposedPorts = Seq(6379))
          container.start()
          container
        }.orDie
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }

  val redis: ZLayer[GenericContainer, Throwable, RedissonClient] =
    ZLayer {
      for {
        container     <- ZIO.service[GenericContainer]
        uri            = s"redis://foobared@${container.host}:${container.mappedPort(container.exposedPorts.head)}"
        redissonConfig = new RedissonConfig()
        _              = redissonConfig.useSingleServer().setAddress(uri)
        client         = Redisson.create(redissonConfig)
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
          p      <- Promise.make[Nothing, Map[Int, Option[PodAddress]]]
          _      <- ZStream.serviceWithStream[Storage](_.assignmentsStream(role)).runForeach(p.succeed(_)).fork
          // saved again until the stream receives it, since it only receives the assignments saved once subscribed
          _      <- (ZIO.serviceWithZIO[Storage](_.saveAssignments(role, expected)) *> ClockLive.sleep(100.millis))
                      .repeatUntilZIO(_ => p.isDone)
          actual <- p.await
        } yield assertTrue(expected == actual)
      }
    ).provideLayerShared(
      container >>> redis ++ ZLayer.succeed(RedisConfig.default) >>> StorageRedis.live
    ) @@ sequential
}
