package example

import shard._
import shard.errors.SendTimeout
import shard.interfaces.PodsHealth
import com.dimafeng.testcontainers.GenericContainer
import example.simple.GuildBehavior
import example.simple.GuildBehavior.{ Guild, GuildFull }
import example.simple.GuildBehavior.GuildMessage.{ Join, Stream, Timeout }
import io.grpc.{ Status, StatusException }
import sage.{ Endpoint, SageConfig, Topology }
import sage.backend.SageClient
import zio.{ Config => _, _ }
import zio.Clock.ClockLive
import zio.test.Assertion._
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._

import java.net.Socket

object EndToEndSpec extends ZIOSpecDefault {

  val shardManagerServer: ZLayer[ShardManager & ManagerConfig & GrpcConfig, Throwable, Unit] =
    ZLayer.scoped(ZIO.serviceWithZIO[ManagerConfig](config => Server.run.forkScoped *> awaitPort(config.apiPort)))

  /**
   * Wait until a server accepts connections on the given port.
   */
  def awaitPort(port: Int): UIO[Unit] =
    ZIO.withClock(ClockLive)(
      ZIO.attemptBlocking(new Socket("localhost", port).close()).retry(Schedule.spaced(50.millis)).orDie
    )

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

  private val config        = ZLayer.succeed(
    Config.default.copy(
      shardManagerPort = 8087,
      simulateRemotePods = true,
      sendTimeout = 3 seconds
    )
  )
  private val grpcConfig    = ZLayer.succeed(GrpcConfig.default)
  private val managerConfig = ZLayer.succeed(ManagerConfig.default.copy(apiPort = 8087))
  private val redisConfig   = ZLayer.succeed(RedisConfig.default)

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("EndToEndSpec")(
      test("Send message to entities") {
        for {
          _       <- Sharding.registerEntity(Guild)(GuildBehavior.behavior)
          _       <- Sharding.registerScoped
          guild   <- Sharding.messenger(Guild)
          _       <- guild.send("guild1")(Join("user1"))
          timeout <- guild.send("guild1")(Timeout()).flip
          _       <- guild.send("guild1")(Join("user2"))
          _       <- guild.send("guild1")(Join("user3"))
          _       <- guild.send("guild1")(Join("user4"))
          members <- guild.send("guild1")(Join("user5"))
          failure <- guild.send("guild1")(Join("user6")).flip
          res     <- guild.stream("guild1")(Stream()).runCollect
        } yield assert(members)(hasSize(equalTo(5))) &&
          assertTrue(failure == GuildFull(5)) &&
          assertTrue(timeout == SendTimeout(Guild.name, "guild1")) &&
          assert(res)(hasSize(equalTo(5)))
      },
      test("Fail the Shard Manager client layer on an invalid host") {
        ZIO
          .scoped(ShardManagerClient.live.build)
          .provide(ZLayer.succeed(Config.default.copy(shardManagerHost = "bad host")), grpcConfig)
          .exit
          .map(assert(_)(fails(anything)))
      },
      test("Time out Shard Manager calls when the Shard Manager doesn't respond") {
        val stalledGrpcConfig =
          GrpcConfig.default.copy(serverInterceptors = Seq(ShardingServerInterceptor.beforeEach(_ => ZIO.never)))
        ZIO.scoped {
          for {
            _      <- Server.run
                        .provideSome[ShardManager](
                          ZLayer.succeed(ManagerConfig.default.copy(apiPort = 8088)),
                          ZLayer.succeed(stalledGrpcConfig)
                        )
                        .forkScoped
            _      <- awaitPort(8088)
            client <- ShardManagerClient.live.build
                        .map(_.get)
                        .provideSome[Scope](
                          ZLayer.succeed(Config.default.copy(shardManagerPort = 8088, shardManagerTimeout = 1 second)),
                          grpcConfig
                        )
            exit   <- client.getAssignments(Role.default).exit.timeoutFail("call not cancelled")(10 seconds)
          } yield assert(exit)(
            fails(
              isSubtype[StatusException](hasField("code", _.getStatus.getCode, equalTo(Status.Code.DEADLINE_EXCEEDED)))
            )
          )
        }
      },
      test("Start the Shard Manager call deadline when the call runs") {
        ZIO.scoped {
          for {
            client <- ShardManagerClient.live.build
                        .map(_.get)
                        .provideSome[Scope](
                          ZLayer.succeed(Config.default.copy(shardManagerPort = 8087, shardManagerTimeout = 1 second)),
                          grpcConfig
                        )
            call    = client.getAssignments(Role.default)
            _      <- ClockLive.sleep(2 seconds)
            exit   <- call.exit
          } yield assert(exit)(succeeds(anything))
        }
      },
      test("Fail the Shard Manager server on an invalid port") {
        Server.run
          .provideSome[ShardManager & GrpcConfig](ZLayer.succeed(ManagerConfig.default.copy(apiPort = -1)))
          .timeout(10 seconds)
          .exit
          .map(assert(_)(fails(anything)))
      }
    ).provideSomeShared[Scope](
      Sharding.live,
      GrpcPods.live,
      ShardManagerClient.live,
      StorageRedis.live,
      ShardManager.live,
      PodsHealth.noop,
      GrpcShardingService.live,
      shardManagerServer,
      container,
      redis,
      config,
      grpcConfig,
      managerConfig,
      redisConfig
    ) @@ sequential @@ withLiveClock
}
