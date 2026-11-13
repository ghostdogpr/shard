package shard.interfaces

import shard.{ PodAddress, Role, ShardId }
import zio.Task

/**
 * An interface to communicate with the Shard Manager API
 */
trait ShardManagerClient {

  /**
   * Register a new registration of the pod, returning the shards it starts with
   */
  def register(podAddress: PodAddress, role: Role, registration: String): Task[Set[ShardId]]

  /**
   * Unregister the given registration of the pod
   */
  def unregister(podAddress: PodAddress, registration: String): Task[Unit]

  def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit]
  def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]
}
