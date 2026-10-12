package shard.interfaces

import shard.{ Pod, PodAddress, Role, ShardId }
import zio.{ Ref, Task, UIO, ZIO, ZLayer }
import zio.stream.{ SubscriptionRef, ZStream }

/**
 * An interface for storing the state of shards, pods and their assignments.
 */
trait Storage {

  /**
   * Get the current state of shard assignments to pods
   */
  def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]

  /**
   * Save the current state of shard assignments to pods
   */
  def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit]

  /**
   * A stream that will emit the state of shard assignments whenever it changes
   */
  def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]]

  /**
   * Get the list of existing pods
   */
  def getPods: Task[Map[PodAddress, Pod]]

  /**
   * Save the list of existing pods
   */
  def savePods(pods: Map[PodAddress, Pod]): Task[Unit]
}

object Storage {

  /**
   * A layer that stores data in-memory, with separate assignments for each role.
   * This is useful for testing with a single pod only.
   */
  val memory: ZLayer[Any, Nothing, Storage] =
    ZLayer {
      for {
        assignmentsRefs <- Ref.Synchronized.make(Map.empty[Role, SubscriptionRef[Map[ShardId, Option[PodAddress]]]])
        podsRef         <- Ref.make(Map.empty[PodAddress, Pod])
      } yield new Storage {
        private def assignmentsRef(role: Role): UIO[SubscriptionRef[Map[ShardId, Option[PodAddress]]]] =
          assignmentsRefs.modifyZIO(refs =>
            refs.get(role) match {
              case Some(ref) => ZIO.succeed((ref, refs))
              case None      =>
                SubscriptionRef.make(Map.empty[ShardId, Option[PodAddress]]).map(ref => (ref, refs.updated(role, ref)))
            }
          )

        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]                       =
          assignmentsRef(role).flatMap(_.get)
        def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit]   =
          assignmentsRef(role).flatMap(_.set(assignments))
        def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          ZStream.unwrap(assignmentsRef(role).map(_.changes))
        def getPods: Task[Map[PodAddress, Pod]]                                                      = podsRef.get
        def savePods(pods: Map[PodAddress, Pod]): Task[Unit]                                         = podsRef.set(pods)
      }
    }
}
