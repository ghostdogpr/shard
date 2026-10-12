package shard

import zio.metrics.Metric
import zio.metrics.Metric.Gauge

object Metrics {
  val shards: Gauge[Double]     = Metric.gauge("shard.shards")
  val entities: Gauge[Double]   = Metric.gauge("shard.entities")
  val singletons: Gauge[Double] = Metric.gauge("shard.singletons")
}
