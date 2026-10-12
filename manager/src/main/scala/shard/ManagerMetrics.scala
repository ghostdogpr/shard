package shard

import zio.metrics.Metric
import zio.metrics.Metric.{ Counter, Gauge }

object ManagerMetrics {
  val pods: Gauge[Double]             = Metric.gauge("shard.pods")
  val assignedShards: Gauge[Double]   = Metric.gauge("shard.shards_assigned")
  val unassignedShards: Gauge[Double] = Metric.gauge("shard.shards_unassigned")

  val rebalances: Counter[Long]       = Metric.counter("shard.rebalances")
  val podHealthChecked: Counter[Long] = Metric.counter("shard.pod_health_checked")
}
