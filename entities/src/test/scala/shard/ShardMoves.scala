package shard

import zio._

extension (sharding: Sharding) {
  def assignShards(shards: Set[ShardId]): UIO[Unit] =
    sharding.registration.someOrElseZIO(ZIO.dieMessage("not registered")).flatMap(sharding.assign(_, shards)).unit

  def unassignShards(shards: Set[ShardId]): UIO[Unit] =
    sharding.registration.someOrElseZIO(ZIO.dieMessage("not registered")).flatMap(sharding.unassign(_, shards)).unit
}
