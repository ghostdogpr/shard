package example

import shard._
import shard.interfaces.{ Pods, PodsHealth, ShardManagerClient, Storage }
import io.grpc.{ Metadata, Status }
import zio.test._
import zio.{ Config => _, _ }

object ShardManagerAuthExampleSpec extends ZIOSpecDefault {

  val validToken = "validBearerToken"

  private val authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

  val shardManagerServerLayer: ZLayer[ManagerConfig, Throwable, Unit] =
    ZLayer.makeSome[ManagerConfig, Unit](
      ZLayer.scoped(
        ZIO.serviceWithZIO[ManagerConfig](config => Server.run.forkScoped *> EndToEndSpec.awaitPort(config.apiPort))
      ),
      ZLayer.succeed(
        GrpcConfig.default.copy(
          serverInterceptors = Seq(
            ShardingServerInterceptor.beforeEach { ctx =>
              val authenticated = Option(ctx.requestMetadata.get(authKey)).contains(s"Bearer $validToken")
              ZIO.unless(authenticated)(ZIO.fail(Status.UNAUTHENTICATED.asException())).unit
            }
          )
        )
      ),
      Storage.memory,
      ShardManager.live,
      Pods.noop,
      PodsHealth.noop
    )

  def clientWithAuthToken(token: String): RIO[Config & Scope, ShardManagerClient] =
    ShardManagerClient.live.build
      .map(_.get)
      .provideSomeLayer[Config & Scope](
        ZLayer.succeed(
          GrpcConfig.default.copy(
            clientInterceptors = Seq(ShardingClientInterceptor.headersUpdater(_.put(authKey, s"Bearer $token")))
          )
        )
      )

  def spec: Spec[TestEnvironment, Any] =
    suite("ShardManagerAuthSpec")(
      test("auth example for shard manager") {
        ZIO.scoped {
          for {
            validClient    <- clientWithAuthToken(validToken)
            invalidClient  <- clientWithAuthToken("invalid")
            validRequest   <- validClient.getAssignments(Role.default).exit
            invalidRequest <- invalidClient.getAssignments(Role.default).exit
          } yield assertTrue(validRequest.isSuccess, invalidRequest.isFailure)
        }
      }
    ).provide(
      shardManagerServerLayer,
      ZLayer.succeed(Config.default),
      ZLayer.succeed(ManagerConfig.default)
    )
}
