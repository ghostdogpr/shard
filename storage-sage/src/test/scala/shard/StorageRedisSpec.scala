package shard

import com.dimafeng.testcontainers.GenericContainer
import sage.{ Endpoint, SageConfig, Topology }
import sage.backend.SageClient
import zio._
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

  val redis: ZLayer[GenericContainer, Throwable, SageClient] =
    ZLayer.scoped {
      for {
        container <- ZIO.service[GenericContainer]
        endpoint   = Endpoint(container.host, container.mappedPort(container.exposedPorts.head))
        client    <- SageClient.scoped(SageConfig(topology = Topology.Standalone(endpoint)))
      } yield client
    }

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StorageRedisSpec")(StorageSuite.tests*).provideLayerShared(
      (container >>> redis) ++ ZLayer.succeed(RedisConfig.default) >>> StorageRedis.live
    ) @@ sequential @@ timeout(1.minute)
}
