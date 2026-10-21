package shard.internal

import shard.{ Config, PodAddress, Role, ShardId }
import shard.interfaces.ShardManagerClient
import zio.{ Task, ZIO, ZLayer }

private[shard] object LocalShardManagerClient {

  val live: ZLayer[Config, Nothing, ShardManagerClient] =
    ZLayer {
      for {
        config <- ZIO.service[Config]
        pod     = PodAddress(config.selfHost, config.shardingPort)
        shards  = (1 to config.numberOfShards).map(_ -> Some(pod)).toMap
      } yield new ShardManagerClient {
        def register(podAddress: PodAddress, role: Role): Task[Unit]           = ZIO.unit
        def unregister(podAddress: PodAddress): Task[Unit]                     = ZIO.unit
        def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit]             = ZIO.unit
        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] = ZIO.succeed(shards)
      }
    }
}
