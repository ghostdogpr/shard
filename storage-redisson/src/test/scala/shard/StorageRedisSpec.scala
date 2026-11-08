package shard

import scala.jdk.CollectionConverters._

import shard.interfaces.Storage
import com.dimafeng.testcontainers.GenericContainer
import org.redisson.Redisson
import org.redisson.config.{ Config => RedissonConfig }
import org.redisson.api.RedissonClient
import org.redisson.client.codec.StringCodec
import zio.Clock.ClockLive
import zio._
import zio.stream.ZStream
import zio.test.TestAspect.{ sequential, timeout }
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

  val redis: ZLayer[GenericContainer, Throwable, RedissonClient] =
    ZLayer {
      for {
        container     <- ZIO.service[GenericContainer]
        uri            = s"redis://${container.host}:${container.mappedPort(container.exposedPorts.head)}"
        redissonConfig = new RedissonConfig()
        _              = redissonConfig.useSingleServer().setAddress(uri)
        client         = Redisson.create(redissonConfig)
      } yield client
    }

  private val role = Role.default

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageRedisSpec")(
      (StorageSuite.tests :+
        test("stores plain strings like the other backends") {
          val pod                                                         = Pod(PodAddress("host1", 1), "1.0.0", role)
          def read(key: String): RIO[RedissonClient, Map[String, String]] =
            ZIO
              .serviceWithZIO[RedissonClient](client =>
                ZIO.fromCompletionStage(client.getMap[String, String](key, StringCodec.INSTANCE).readAllMapAsync())
              )
              .map(_.asScala.toMap)
          for {
            _           <- ZIO.serviceWithZIO[Storage](_.savePods(Set(pod)))
            _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(role, Map(1 -> Some(pod.address), 2 -> None)))
            pods        <- read(RedisConfig.default.podsKey)
            assignments <- read(RedisConfig.default.assignmentsKey(role))
          } yield assertTrue(
            pods == Map("host1:1" -> "1.0.0 default"),
            assignments == Map("1" -> "host1:1", "2" -> "")
          )
        } :+
        test("the assignments stream removes its listener when it ends") {
          val listeners = ZIO.serviceWith[RedissonClient](
            _.getTopic(RedisConfig.default.assignmentsKey(role), StringCodec.INSTANCE).countListeners
          )
          for {
            before <- listeners
            fiber  <- ZStream.serviceWithStream[Storage](_.assignmentsStream(role)).runDrain.fork
            _      <- (ClockLive.sleep(10.millis) *> listeners).repeatUntil(_ > before)
            _      <- fiber.interrupt
            after  <- listeners
          } yield assertTrue(after == before)
        })*
    ).provideLayerShared(
      (container >>> redis) >+> (ZLayer.succeed(RedisConfig.default) >+> StorageRedis.live)
    ) @@ sequential @@ timeout(1.minute)
}
