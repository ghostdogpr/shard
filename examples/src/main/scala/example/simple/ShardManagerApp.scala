package example.simple

import zio._

import shard._
import shard.interfaces._

object ShardManagerApp extends ZIOAppDefault {
  def run: Task[Nothing] =
    Server.run
      .provide(
        ZLayer.succeed(ManagerConfig.default),
        ZLayer.succeed(GrpcConfig.default),
        PodsHealth.local, // just ping a pod to see if it's alive
        GrpcPods.live,    // use gRPC protocol
        Storage.memory,   // store data in memory
        ShardManager.live // Shard Manager logic
      )
}
