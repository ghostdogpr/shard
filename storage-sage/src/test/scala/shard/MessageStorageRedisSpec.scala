package shard

import java.net.InetAddress

import com.dimafeng.testcontainers.GenericContainer
import sage.{Endpoint, SageConfig, Topology}
import sage.backend.SageClient
import zio._
import zio.test._
import zio.test.TestAspect.{sequential, timeout, withLiveClock}

import shard.interfaces.MessageStorage

object MessageStorageRedisSpec extends ZIOSpecDefault {

  // a cluster of one node that owns every slot, which rejects the commands and scripts whose keys aren't in one slot
  val cluster: ZLayer[GenericContainer, Nothing, SageClient] =
    ZLayer.scoped {
      for {
        container <- ZIO.service[GenericContainer]
        redisCli   = (args: Seq[String]) => ZIO.attemptBlocking(container.execInContainer(("redis-cli" +: args)*).getStdout.trim).orDie
        ip        <- ZIO.attemptBlocking(InetAddress.getByName(container.host).getHostAddress).orDie
        // so that the node announces the address that the tests reach it on
        setup      = List(
                       Seq("config", "set", "cluster-announce-ip", ip),
                       Seq("config", "set", "cluster-announce-port", container.mappedPort(6379).toString),
                       Seq("cluster", "addslotsrange", "0", "16383")
                     )
        _         <- ZIO.foreachDiscard(setup)(args => redisCli(args).filterOrDieMessage(_ == "OK")(s"redis-cli ${args.mkString(" ")} failed"))
        _         <- ZIO
                       .collectFirst(1 to 100)(_ =>
                         (ZIO.attemptBlocking(Thread.sleep(100)).orDie *> redisCli(Seq("cluster", "info")))
                           .map(info => Option.when(info.contains("cluster_state:ok"))(()))
                       )
                       .someOrElseZIO(ZIO.dieMessage("The cluster isn't ready"))
        endpoint   = Endpoint(container.host, container.mappedPort(6379))
        client    <- SageClient.scoped(SageConfig(topology = Topology.Cluster(Vector(endpoint)))).orDie
      } yield client
    }

  private def storage(redis: ZLayer[Any, Throwable, SageClient]): ZLayer[Any, Throwable, MessageStorage] =
    redis ++ ZLayer.succeed(RedisConfig.default) >>> MessageStorageRedis.live

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("MessageStorageRedisSpec")(
      suite("standalone")(MessageStorageSuite.tests*).provideLayerShared(storage(StorageRedisSpec.container() >>> StorageRedisSpec.redis)),
      suite("cluster")(MessageStorageSuite.tests*)
        .provideLayerShared(storage(StorageRedisSpec.container("redis-server", "--cluster-enabled", "yes") >>> cluster))
    ) @@ sequential @@ withLiveClock @@ timeout(1.minute)
}
