package example.simple

import shard._
import shard.interfaces._
import example.simple.GuildBehavior._
import example.simple.GuildBehavior.GuildMessage.Join
import zio.{ Config => _, _ }

object GuildApp extends ZIOAppDefault {
  val program =
    for {
      _     <- Sharding.registerEntity(Guild)(behavior)
      _     <- Sharding.registerScoped
      guild <- Sharding.messenger(Guild)
      _     <- guild.send("guild1")(Join("user1")).either.debug
      _     <- guild.send("guild1")(Join("user2")).either.debug
      _     <- guild.send("guild1")(Join("user3")).either.debug
      _     <- guild.send("guild1")(Join("user4")).either.debug
      _     <- guild.send("guild1")(Join("user5")).either.debug
      _     <- guild.send("guild1")(Join("user6")).either.debug
    } yield ()

  def run: Task[Unit] =
    ZIO
      .scoped(program)
      .provide(
        ZLayer.succeed(Config.default),
        ZLayer.succeed(GrpcConfig.default),
        Storage.memory,
        ShardManagerClient.live,
        GrpcPods.live,
        Sharding.live,
        GrpcShardingService.live
      )
}
