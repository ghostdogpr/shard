package shard.errors

import shard.PodAddress

/**
 * Exception indicating that a pod is not responsive.
 */
case class PodUnavailable(pod: PodAddress) extends Exception(s"Pod $pod is unavailable")
