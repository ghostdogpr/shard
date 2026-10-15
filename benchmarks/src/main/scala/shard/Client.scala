package shard

import shard.Server.Message.{ Ping, StreamPing }
import shard.Server.PingPongEntity
import zio.{ Config => _, _ }

object Client {
  // self host should not be `localhost` to avoid optimization
  private val config = ZLayer.succeed(Config.default.copy(selfHost = "not-localhost"))

  def send(count: Int, parallelism: Int): Task[Unit] =
    ZIO
      .scoped[Sharding](
        for {
          ping <- Sharding.messenger(PingPongEntity)
          _    <- ZIO.foreachParDiscard(1 to count)(_ => ping.send("ping")(Ping("ping"))).withParallelism(parallelism)
        } yield ()
      )
      .provide(config, Server.sharding)

  def sendStream(streams: Int, messagesPerStream: Int, parallelism: Int): Task[Unit] =
    ZIO
      .scoped[Sharding](
        for {
          ping <- Sharding.messenger(PingPongEntity)
          _    <-
            ZIO
              .foreachParDiscard(1 to streams)(_ => ping.stream("ping")(StreamPing("ping", messagesPerStream)).runDrain)
              .withParallelism(parallelism)
        } yield ()
      )
      .provide(config, Server.sharding)
}
