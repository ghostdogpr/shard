package shard

sealed trait ShardingEvent
object ShardingEvent {
  case class ShardsAssigned(podAddress: PodAddress, role: Role, shards: Set[ShardId])   extends ShardingEvent {
    override def toString: String =
      s"ShardsAssigned(pod=$podAddress, role=${role.name}, shards=${renderShardIds(shards)})"
  }
  case class ShardsUnassigned(podAddress: PodAddress, role: Role, shards: Set[ShardId]) extends ShardingEvent {
    override def toString: String =
      s"ShardsUnassigned(pod=$podAddress, role=${role.name}, shards=${renderShardIds(shards)})"
  }
  case class PodRegistered(pod: Pod)                                                    extends ShardingEvent
  case class PodUnregistered(pod: Pod)                                                  extends ShardingEvent
  case class PodHealthChecked(pod: Pod)                                                 extends ShardingEvent
}
