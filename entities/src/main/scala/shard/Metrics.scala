package shard

import zio.metrics.Metric
import zio.metrics.Metric.{Counter, Gauge}

object Metrics {
  val shards: Gauge[Double]          = Metric.gauge("shard.shards")
  val entities: Gauge[Double]        = Metric.gauge("shard.entities")
  val singletons: Gauge[Double]      = Metric.gauge("shard.singletons")
  val pendingMessages: Gauge[Double] = Metric.gauge("shard.pending_messages")
  val deadLetters: Counter[Long]     = Metric.counter("shard.dead_letters")
}
