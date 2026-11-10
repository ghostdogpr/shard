package shard

import shard.ShardManager._
import shard.interfaces._

import java.time.OffsetDateTime
import scala.util.Random
import zio._
import zio.stream.ZStream

import scala.annotation.tailrec

/**
 * A component in charge of assigning and unassigning shards to/from pods
 */
class ShardManager private (
  stateRef: Ref.Synchronized[Map[Role, ShardManagerState]],
  rebalanceSemaphores: Role => UIO[Semaphore],
  eventsHub: Hub[ShardingEvent],
  // one write at a time per storage key, so that the last one holds the latest state
  podsLock: Semaphore,
  assignmentsLocks: Role => UIO[Semaphore],
  scope: Scope,
  healthApi: PodsHealth,
  podApi: Pods,
  stateRepository: Storage,
  config: ManagerConfig
) {

  def getAssignments(role: Role): UIO[Map[ShardId, Option[PodAddress]]] =
    stateRef.get.map(_.get(role).fold(Map.empty[ShardId, Option[PodAddress]])(_.shards))

  def getShardingEvents: ZStream[Any, Nothing, ShardingEvent] =
    ZStream.fromHub(eventsHub)

  def register(pod: Pod): Task[Unit] =
    ZIO.ifZIO(healthApi.isAlive(pod))(
      onTrue = for {
        _         <- ZIO.logInfo(s"Registering $pod")
        cdt       <- ZIO.succeed(OffsetDateTime.now())
        persisted <-
          modifyStates { states =>
            // an address hosts a single process: a pod of another role registered there is gone
            val (replaced, others) = removePod(states, pod.address, _.pod.role != pod.role)
            val current            = others.getOrElse(pod.role, ShardManagerState(config.numberOfShards(pod.role)))
            val state              = current.copy(pods = current.pods.updated(pod.address, PodWithMetadata(pod, cdt)))
            ((replaced, state.unassignedShards.nonEmpty), others.updated(pod.role, state))
          }.flatMap { case (replaced, triggerRebalance) =>
            ZIO.foreachDiscard(replaced)(announceUnregistered) *>
              eventsHub.publish(ShardingEvent.PodRegistered(pod)) *>
              ZIO.whenDiscard(triggerRebalance)(rebalance(pod.role, rebalanceImmediately = false).background) *>
              persistPods.background
          }.uninterruptible
        // a client that stops waiting doesn't cancel the write
        _         <- persisted.join
      } yield (),
      onFalse = ZIO.logWarning(s"Pod $pod requested to register but is not alive, ignoring") *>
        ZIO.fail(new RuntimeException(s"Pod $pod is not healthy, refusing to register"))
    )

  private def findPod(podAddress: PodAddress): UIO[Option[PodWithMetadata]] =
    stateRef.get.map(_.values.iterator.flatMap(_.pods.get(podAddress)).nextOption())

  // returns the removed pod with the shards it had
  private def removePod(
    states: Map[Role, ShardManagerState],
    podAddress: PodAddress,
    matches: PodWithMetadata => Boolean
  ): (Option[(Pod, Set[ShardId])], Map[Role, ShardManagerState]) = {
    val found = states.iterator.flatMap { case (role, state) =>
      state.pods.get(podAddress).filter(matches).map(pod => (role, state, pod.pod))
    }.nextOption()
    found.fold((Option.empty[(Pod, Set[ShardId])], states)) { case (role, state, pod) =>
      val shards      = state.shards.collect { case (shard, Some(p)) if p == podAddress => shard }.toSet
      val shardsAfter = state.shards ++ shards.map(_ -> None)
      (Some((pod, shards)), states.updated(role, state.copy(pods = state.pods - podAddress, shards = shardsAfter)))
    }
  }

  def notifyUnhealthyPod(podAddress: PodAddress, ignoreMetric: Boolean = false): UIO[Unit] =
    ZIO
      .whenCaseZIODiscard(findPod(podAddress)) { case Some(registered) =>
        ManagerMetrics.podHealthChecked.tagged("pod_address", podAddress.toString).increment.unless(ignoreMetric) *>
          eventsHub.publish(ShardingEvent.PodHealthChecked(registered.pod)) *>
          ZIO.unlessZIO(healthApi.isAlive(registered.pod))(
            ZIO.logWarning(s"Pod $podAddress is not alive, unregistering") *>
              // a pod that registered again at this address in the meantime stays
              unregisterIf(podAddress)(_ == registered)
          )
      }

  def checkAllPodsHealth: UIO[Unit] =
    for {
      pods <- stateRef.get.map(_.values.flatMap(_.pods.keySet))
      _    <- ZIO.foreachParDiscard(pods)(notifyUnhealthyPod(_, ignoreMetric = true)).withParallelism(4)
    } yield ()

  def unregister(podAddress: PodAddress): UIO[Unit] =
    unregisterIf(podAddress)(_ => true)

  // check and remove in one update, so that concurrent calls unregister the pod once
  private def unregisterIf(podAddress: PodAddress)(matches: PodWithMetadata => Boolean): UIO[Unit] =
    modifyStates(removePod(_, podAddress, matches))
      .flatMap(ZIO.foreachDiscard(_)((pod, shards) => announceUnregistered(pod, shards) *> persistPods.background))
      .uninterruptible

  // all state changes go through here, so that the gauges always match the state
  private def modifyStates[A](f: Map[Role, ShardManagerState] => (A, Map[Role, ShardManagerState])): UIO[A] =
    stateRef.modifyZIO { before =>
      val (result, after) = f(before)
      ZIO
        .foreachDiscard(after)((role, state) =>
          ZIO.unlessDiscard(before.get(role).exists(_ eq state))(recordGauges(role, before.get(role), state))
        )
        .as((result, after))
    }.uninterruptible

  private def announceUnregistered(pod: Pod, unassignments: Set[ShardId]): UIO[Unit] =
    for {
      _ <- ZIO.logInfo(s"Unregistered ${pod.address}")
      _ <- eventsHub.publish(ShardingEvent.PodUnregistered(pod))
      _ <- eventsHub
             .publish(ShardingEvent.ShardsUnassigned(pod.address, pod.role, unassignments))
             .when(unassignments.nonEmpty)
      // the rebalance doesn't persist when no pod of the role is left
      _ <- persistAssignments(pod.role).background.when(unassignments.nonEmpty)
      _ <- rebalance(pod.role, rebalanceImmediately = true).background
    } yield ()

  extension [A](effect: UIO[A]) {
    // stopped when the Shard Manager is released, even when forked from uninterruptible code
    private def background: UIO[Fiber.Runtime[Nothing, A]] = effect.interruptible.forkIn(scope)
  }

  private def rebalance(role: Role, rebalanceImmediately: Boolean): UIO[Unit] =
    rebalanceSemaphores(role).flatMap(_.withPermit(rebalanceWithPermit(role, rebalanceImmediately)))

  // skips the role while another of its rebalances is running
  private def rebalanceIfIdle(role: Role): UIO[Unit] =
    rebalanceSemaphores(role)
      .flatMap(_.tryWithPermit(rebalanceWithPermit(role, rebalanceImmediately = false)))
      .unit

  private def rebalanceWithPermit(role: Role, rebalanceImmediately: Boolean): UIO[Unit] =
    for {
      state                       <- stateRef.get.map(_.getOrElse(role, ShardManagerState(config.numberOfShards(role))))
      // find which shards to assign and unassign
      (assignments, unassignments) = if (rebalanceImmediately || state.unassignedShards.nonEmpty)
                                       decideAssignmentsForUnassignedShards(state)
                                     else decideAssignmentsForUnbalancedShards(state, config.rebalanceRate)
      areChanges                   = assignments.nonEmpty || unassignments.nonEmpty
      _                           <- (ZIO.logDebug(s"Rebalancing role ${role.name} (rebalanceImmediately=$rebalanceImmediately)") *>
                                       ManagerMetrics.rebalances.tagged("role", role.name).increment).when(areChanges)
      // ping pods first to make sure they are ready and remove those who aren't
      failedPingedPods            <- ZIO
                                       .foreachPar(assignments.keySet ++ unassignments.keySet)(podAddress =>
                                         podApi
                                           .ping(podAddress)
                                           .timeout(config.pingTimeout)
                                           .someOrFailException
                                           .fold(_ => Set(podAddress), _ => Set.empty[PodAddress])
                                       )
                                       .map(_.flatten)
      shardsToRemove               = assignments.collect {
                                       case (podAddress, shards) if failedPingedPods.contains(podAddress) => shards
                                     }.toSet.flatten ++
                                       unassignments.collect {
                                         case (podAddress, shards) if failedPingedPods.contains(podAddress) => shards
                                       }.toSet.flatten
      readyAssignments             = assignments.view.mapValues(_ diff shardsToRemove).filterNot(_._2.isEmpty).toMap
      readyUnassignments           = unassignments.view.mapValues(_ diff shardsToRemove).filterNot(_._2.isEmpty).toMap
      // do the unassignments first
      failedUnassignments         <- moveShards(role, readyUnassignments, assign = false)
      failedUnassignedPods         = failedUnassignments.map(_._1)
      failedUnassignedShards       = failedUnassignments.flatMap(_._2).toSet
      // remove assignments of shards that couldn't be unassigned, as well as faulty pods
      filteredAssignments          = (readyAssignments -- failedUnassignedPods).map { case (podAddress, shards) =>
                                       podAddress -> (shards diff failedUnassignedShards)
                                     }
      // then do the assignments
      failedAssignments           <- moveShards(role, filteredAssignments, assign = true)
      failedPods                   = failedPingedPods ++ failedUnassignedPods ++ failedAssignments.map(_._1)
      // check if failing pods are still up
      _                           <- ZIO.foreachDiscard(failedPods)(notifyUnhealthyPod(_)).background.when(failedPods.nonEmpty)
      _                           <- ZIO.logWarning(s"Failed to rebalance pods for role ${role.name}: $failedPods").when(failedPods.nonEmpty)
      // retry rebalancing later if there was any failure
      _                           <- (Clock.sleep(config.rebalanceRetryInterval) *> rebalance(role, rebalanceImmediately)).background
                                       .when(failedPods.nonEmpty && rebalanceImmediately)
      // persist state changes to Redis
      _                           <- persistAssignments(role).when(areChanges)
    } yield ()

  private def withRetry[A](zio: Task[A]): UIO[Unit] =
    zio
      .retry[Any, Any](Schedule.spaced(config.persistRetryInterval) && Schedule.recurs(config.persistRetryCount))
      .orDie
      .unit

  private def persistAssignments(role: Role): UIO[Unit] =
    assignmentsLocks(role).flatMap(
      _.withPermit(withRetry(getAssignments(role).flatMap(stateRepository.saveAssignments(role, _))))
    )

  // keys are written independently so that a failing or stuck write doesn't block the others,
  // and `disconnect` lets the timeout end the release even if a write ignores interruption
  private def persistAll: UIO[Unit] =
    stateRef.get
      .flatMap(states =>
        ZIO.foreachParDiscard(states.keys)(role =>
          persistAssignments(role).catchAllCause(
            ZIO.logWarningCause(s"Failed to persist assignments for role ${role.name} on shutdown", _)
          )
        ) <&> persistPods.catchAllCause(ZIO.logWarningCause("Failed to persist pods on shutdown", _))
      )
      .disconnect
      .interruptible
      .timeout(config.shutdownPersistTimeout)
      .someOrElseZIO(ZIO.logWarning(s"Persisting on shutdown timed out after ${config.shutdownPersistTimeout.render}"))

  private def persistPods: UIO[Unit] =
    podsLock.withPermit(
      withRetry(
        stateRef.get.flatMap(states => stateRepository.savePods(states.values.flatMap(_.pods.values.map(_.pod)).toSet))
      )
    )

  // once the pod has been called, the state is updated even on shutdown (unless an assigned pod left meanwhile)
  // and only the shards that actually moved are announced; returns the failed moves
  private def moveShards(
    role: Role,
    moves: Map[PodAddress, Set[ShardId]],
    assign: Boolean
  ): UIO[List[(PodAddress, Set[ShardId])]] = {
    val call  = if (assign) podApi.assignShards else podApi.unassignShards
    val event = if (assign) ShardingEvent.ShardsAssigned.apply else ShardingEvent.ShardsUnassigned.apply
    ZIO.filterNotPar(moves.toList) { (podAddress, shards) =>
      val target = Option.when(assign)(podAddress)
      ZIO.uninterruptibleMask(restore =>
        restore(call(podAddress, role, shards)).foldZIO(
          _ => ZIO.succeed(false),
          _ =>
            modifyStates(states =>
              states.get(role).filter(state => target.forall(state.pods.contains)) match {
                case Some(state) =>
                  val changed = shards.filter(shard => state.shards.get(shard).exists(_ != target))
                  (Some(changed), states.updated(role, state.copy(shards = state.shards ++ changed.map(_ -> target))))
                case None        => (None, states)
              }
            ).flatMap(
              ZIO
                .foreach(_)(changed => eventsHub.publish(event(podAddress, role, changed)).when(changed.nonEmpty))
                .map(_.isDefined)
            )
        )
      )
    }
  }
}

object ShardManager {

  /**
   * A layer that starts the Shard Manager process
   */
  val live: ZLayer[PodsHealth & Pods & Storage & ManagerConfig, Throwable, ShardManager] =
    ZLayer.scoped {
      for {
        config              <- ZIO.service[ManagerConfig]
        stateRepository     <- ZIO.service[Storage]
        healthApi           <- ZIO.service[PodsHealth]
        podApi              <- ZIO.service[Pods]
        oldPods             <- stateRepository.getPods
        // remove unhealthy pods on startup
        pods                <- ZIO.filterPar(oldPods)(healthApi.isAlive)
        failedPods           = oldPods -- pods
        _                   <- ZIO.whenDiscard(failedPods.nonEmpty)(
                                 ZIO.logInfo(s"Ignoring pods that are no longer alive ${failedPods.mkString("[", ", ", "]")}")
                               )
        _                   <- ZIO.logInfo(s"Recovered pods ${pods.mkString("[", ", ", "]")}")
        cdt                 <- ZIO.succeed(OffsetDateTime.now())
        // the roles of dead pods too, whose shards are then all unassigned
        initialStates       <- ZIO
                                 .foreach(oldPods.groupBy(_.role)) { (role, stored) =>
                                   val rolePods = stored.filter(pods).map(p => p.address -> PodWithMetadata(p, cdt)).toMap
                                   for {
                                     oldAssignments       <- stateRepository.getAssignments(role)
                                     (assignments, failed) = oldAssignments.partition(_._2.exists(rolePods.contains))
                                     failedAssignments     = failed.collect { case (shard, Some(addr)) => shard -> addr }
                                     _                    <-
                                       ZIO.whenDiscard(failedAssignments.nonEmpty)(
                                         ZIO.logWarning(
                                           s"Ignoring assignments for pods that are no longer alive for role ${role.name}: ${failedAssignments
                                               .mkString("[", ", ", "]")}"
                                         )
                                       )
                                     _                    <-
                                       ZIO.logInfo(
                                         s"Recovered assignments for role ${role.name}: ${assignments.mkString("[", ", ", "]")}"
                                       )
                                   } yield role -> ShardManagerState(
                                     rolePods,
                                     ShardManagerState(config.numberOfShards(role)).shards ++ assignments
                                   )
                                 }
        _                   <- ZIO.foreachDiscard(initialStates)(recordGauges(_, None, _))
        state               <- Ref.Synchronized.make(initialStates)
        rebalanceSemaphores <- semaphorePerRole
        eventsHub           <- Hub.unbounded[ShardingEvent]
        podsLock            <- Semaphore.make(1)
        assignmentsLocks    <- semaphorePerRole
        // not the layer scope, so that its closing can be bounded
        scope               <- Scope.make
        shardManager         = new ShardManager(
                                 stateRef = state,
                                 rebalanceSemaphores = rebalanceSemaphores,
                                 eventsHub = eventsHub,
                                 podsLock = podsLock,
                                 assignmentsLocks = assignmentsLocks,
                                 scope = scope,
                                 healthApi = healthApi,
                                 podApi = podApi,
                                 stateRepository = stateRepository,
                                 config = config
                               )
        _                   <- ZIO.addFinalizer(
                                 scope
                                   .close(Exit.unit)
                                   .disconnect
                                   .interruptible
                                   .timeout(config.shutdownPersistTimeout)
                                   .someOrElseZIO(ZIO.logWarning("Background tasks did not stop on shutdown")) *>
                                   shardManager.persistAll
                               )
        _                   <- shardManager.persistPods
        // rebalance immediately if there are unassigned shards
        _                   <- ZIO.foreachDiscard(initialStates) { case (role, state) =>
                                 shardManager.rebalance(role, rebalanceImmediately = state.unassignedShards.nonEmpty).forkIn(scope)
                               }
        // start a regular rebalance at the given interval, per role so that a slow one doesn't delay the others
        _                   <- state.get
                                 .flatMap(states => ZIO.foreachDiscard(states.keySet)(shardManager.rebalanceIfIdle(_).fork))
                                 .repeat(Schedule.spaced(config.rebalanceInterval))
                                 .forkIn(scope)
        _                   <- shardManager.getShardingEvents.mapZIO(event => ZIO.logInfo(event.toString)).runDrain.forkIn(scope)
        _                   <- shardManager.checkAllPodsHealth.repeat(Schedule.spaced(config.podHealthCheckInterval)).forkIn(scope)
        _                   <- ZIO.logInfo("Shard Manager loaded")
      } yield shardManager
    }

  // uninterruptible: memoize never completes a role's entry if its first caller is interrupted before creating it
  private val semaphorePerRole: UIO[Role => UIO[Semaphore]] =
    ZIO.memoize((_: Role) => Semaphore.make(1)).map(semaphore => semaphore(_).uninterruptible)

  private def recordGauges(role: Role, before: Option[ShardManagerState], after: ShardManagerState): UIO[Unit] = {
    val previous = before.fold(Map.empty[PodAddress, Set[ShardId]])(_.shardsPerPod)
    ManagerMetrics.pods.tagged("role", role.name).set(after.pods.size) *>
      ManagerMetrics.unassignedShards.tagged("role", role.name).set(after.unassignedShards.size) *>
      ZIO.foreachDiscard(previous.keySet ++ after.shardsPerPod.keySet) { podAddress =>
        val count = after.shardsPerPod.get(podAddress).fold(0)(_.size)
        // only changed counts, so a pod that never had shards has no series
        ZIO.unlessDiscard(previous.get(podAddress).fold(0)(_.size) == count)(
          ManagerMetrics.assignedShards.tagged("role", role.name).tagged("pod_address", podAddress.toString).set(count)
        )
      }
  }

  implicit def listOrder[A](implicit ev: Ordering[A]): Ordering[List[A]] = (xs: List[A], ys: List[A]) => {
    @tailrec def loop(xs: List[A], ys: List[A]): Int =
      xs match {
        case Nil     =>
          if (ys.isEmpty) 0 else -1
        case x :: xs =>
          ys match {
            case Nil     => 1
            case y :: ys =>
              val n = ev.compare(x, y)
              if (n != 0) n else loop(xs, ys)
          }
      }

    if (xs eq ys) 0 else loop(xs, ys)
  }

  case class ShardManagerState(pods: Map[PodAddress, PodWithMetadata], shards: Map[ShardId, Option[PodAddress]]) {
    lazy val unassignedShards: Set[ShardId]    = shards.collect { case (k, None) => k }.toSet
    lazy val averageShardsPerPod: ShardId      = if (pods.nonEmpty) shards.size / pods.size else 0
    private lazy val podVersions               = pods.values.toList.map(extractVersion)
    lazy val maxVersion: Option[List[ShardId]] = podVersions.maxOption
    lazy val allPodsHaveMaxVersion: Boolean    = podVersions.forall(maxVersion.contains)

    lazy val shardsPerPod: Map[PodAddress, Set[ShardId]] =
      pods.map { case (k, _) => k -> Set.empty[ShardId] } ++
        shards.groupBy(_._2).collect { case (Some(address), shards) => address -> shards.keySet }
  }

  object ShardManagerState {
    def apply(numberOfShards: Int): ShardManagerState =
      ShardManagerState(Map.empty, (1 to numberOfShards).map(_ -> None).toMap)
  }

  case class PodWithMetadata(pod: Pod, registeredAt: OffsetDateTime)

  def decideAssignmentsForUnassignedShards(
    state: ShardManagerState
  ): (Map[PodAddress, Set[ShardId]], Map[PodAddress, Set[ShardId]]) =
    pickNewPods(state.unassignedShards.toList, state, rebalanceImmediately = true, 1.0)

  def decideAssignmentsForUnbalancedShards(
    state: ShardManagerState,
    rebalanceRate: Double
  ): (Map[PodAddress, Set[ShardId]], Map[PodAddress, Set[ShardId]]) = {
    val extraShardsToAllocate   =
      if (state.allPodsHaveMaxVersion) { // don't do regular rebalance in the middle of a rolling update
        state.shardsPerPod.flatMap { case (_, shards) =>
          // count how many extra shards compared to the average
          val extraShards = (shards.size - state.averageShardsPerPod).max(0)
          Random.shuffle(shards).take(extraShards)
        }.toSet
      } else Set.empty
    val sortedShardsToRebalance = extraShardsToAllocate.toList.sortBy { shard =>
      // handle unassigned shards first, then shards on the pods with most shards, then shards on old pods
      state.shards.get(shard).flatten.fold((Int.MinValue, OffsetDateTime.MIN)) { podAddress =>
        (
          state.shardsPerPod.get(podAddress).fold(Int.MinValue)(-_.size),
          state.pods.get(podAddress).fold(OffsetDateTime.MIN)(_.registeredAt)
        )
      }
    }
    pickNewPods(sortedShardsToRebalance, state, rebalanceImmediately = false, rebalanceRate)
  }

  private def pickNewPods(
    shardsToRebalance: List[ShardId],
    state: ShardManagerState,
    rebalanceImmediately: Boolean,
    rebalanceRate: Double
  ): (Map[PodAddress, Set[ShardId]], Map[PodAddress, Set[ShardId]]) = {
    val (_, assignments)    = shardsToRebalance.foldLeft((state.shardsPerPod, List.empty[(ShardId, PodAddress)])) {
      case ((shardsPerPod, assignments), shard) =>
        val unassignedPods = assignments.flatMap { case (shard, _) =>
          state.shards.get(shard).flatten[PodAddress]
        }.toSet
        // find pod with least amount of shards
        shardsPerPod
          // keep only pods with the max version
          .filter { case (podAddress, _) =>
            state.maxVersion.forall(max => state.pods.get(podAddress).map(extractVersion).forall(_ == max))
          }
          // don't assign too many shards to the same pods, unless we need rebalance immediately
          .filter { case (podAddress, _) =>
            rebalanceImmediately ||
            assignments.count { case (_, p) => p == podAddress } < state.shards.size * rebalanceRate
          }
          // don't assign to a pod that was unassigned in the same rebalance
          .filterNot { case (podAddress, _) => unassignedPods.contains(podAddress) }
          .minByOption(_._2.size) match {
          case Some((podAddress, shards)) =>
            val oldPodAddress = state.shards.get(shard).flatten
            // if old pod is same as new pod, don't change anything
            if (oldPodAddress.contains(podAddress)) (shardsPerPod, assignments)
            // if the new pod has more, as much, or only 1 less shard than the old pod, don't change anything
            else if (
              shardsPerPod.get(podAddress).fold(0)(_.size) + 1 >= oldPodAddress.fold(Int.MaxValue)(
                shardsPerPod.getOrElse(_, Nil).size
              )
            ) (shardsPerPod, assignments)
            // otherwise, create a new assignment
            else {
              val unassigned =
                oldPodAddress.fold(shardsPerPod)(oldPodAddress =>
                  shardsPerPod.updatedWith(oldPodAddress)(_.map(_ - shard))
                )
              (unassigned.updated(podAddress, shards + shard), (shard, podAddress) :: assignments)
            }
          case None                       => (shardsPerPod, assignments)
        }
    }
    val unassignments       = assignments.flatMap { case (shard, _) => state.shards.get(shard).flatten.map(shard -> _) }
    val assignmentsPerPod   = assignments.groupBy(_._2).map { case (k, v) => k -> v.map(_._1).toSet }
    val unassignmentsPerPod = unassignments.groupBy(_._2).map { case (k, v) => k -> v.map(_._1).toSet }
    (assignmentsPerPod, unassignmentsPerPod)
  }

  private def extractVersion(pod: PodWithMetadata): List[Int] =
    pod.pod.version.split("[.-]").toList.flatMap(_.toIntOption)
}
