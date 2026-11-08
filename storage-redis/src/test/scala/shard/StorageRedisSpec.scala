package shard

import shard.StorageRedis.Redis
import com.dimafeng.testcontainers.GenericContainer
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.connection.RedisClient
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.effect.Log
import dev.profunktor.redis4cats.pubsub.PubSub
import zio._
import zio.interop.catz._
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

  val redis: ZLayer[GenericContainer, Throwable, Redis] =
    ZLayer.scopedEnvironment {
      implicit val runtime: zio.Runtime[Any] = zio.Runtime.default
      implicit val logger: Log[Task]         = new Log[Task] {
        override def debug(msg: => String): Task[Unit] = ZIO.unit
        override def error(msg: => String): Task[Unit] = ZIO.logError(msg)
        override def info(msg: => String): Task[Unit]  = ZIO.logDebug(msg)
      }

      ZIO
        .service[GenericContainer]
        .flatMap(container =>
          (for {
            client   <- RedisClient[Task].from(
                          s"redis://${container.host}:${container.mappedPort(container.exposedPorts.head)}"
                        )
            commands <- Redis[Task].fromClient(client, RedisCodec.Utf8)
            pubSub   <- PubSub.mkPubSubConnection[Task, String, String](client, RedisCodec.Utf8)
          } yield ZEnvironment(commands, pubSub)).toScopedZIO
        )
    }

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageRedisSpec")(StorageSuite.tests*).provideLayerShared(
      container >>> redis ++ ZLayer.succeed(RedisConfig.default) >>> StorageRedis.live
    ) @@ sequential @@ timeout(1.minute)
}
