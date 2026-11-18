package shard.interfaces

import zio.Task

import shard.{PodAddress, Role, ShardId}

/**
  * An interface to communicate with the Shard Manager API
  */
trait ShardManagerClient {
  def register(podAddress: PodAddress, role: Role): Task[Unit]
  def unregister(podAddress: PodAddress): Task[Unit]
  def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit]
  def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]
}
