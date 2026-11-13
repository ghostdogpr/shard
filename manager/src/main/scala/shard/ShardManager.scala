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
    findPod(pod.address).flatMap { before =>
      ZIO.ifZIO(healthApi.isAlive(pod) && isCurrent(pod))(
        onTrue = for {
          _         <- ZIO.logInfo(s"Registering $pod")
          cdt       <- ZIO.succeed(OffsetDateTime.now())
          persisted <-
            modifyStates { states =>
              // checked against what the address had before: check again if it changed
              if (podAt(states, pod.address) != before) (None, states)
              else {
                // an address hosts a single registration: an earlier one, or one of another role, is gone
                val (replaced, others) =
                  removePod(states, pod.address, p => p.pod.role != pod.role || p.pod.registration != pod.registration)
                val current            = others.getOrElse(pod.role, ShardManagerState(config.numberOfShards(pod.role)))
                val state              = current.copy(pods = current.pods.updated(pod.address, PodWithMetadata(pod, cdt)))
                (Some((replaced, state.unassignedShards.nonEmpty)), others.updated(pod.role, state))
              }
            }.flatMap(ZIO.foreach(_) { case (replaced, triggerRebalance) =>
              ZIO.foreachDiscard(replaced)(announceUnregistered) *>
                eventsHub.publish(ShardingEvent.PodRegistered(pod)) *>
                ZIO
                  .whenDiscard(triggerRebalance)(rebalance(pod.role, rebalanceImmediately = false).background(scope)) *>
                persistPods.background(scope)
            }).uninterruptible
          // a client that stops waiting doesn't cancel the write
          _         <- persisted.fold(register(pod))(_.join)
        } yield (),
        onFalse = ZIO.logWarning(s"Pod $pod requested to register but is not alive or not registering, ignoring") *>
          ZIO.fail(new RuntimeException(s"Pod $pod is not healthy or not registering, refusing to register"))
      )
    }

  // a register request may arrive after the pod registered again: only the pod knows its current registration
  private def isCurrent(pod: Pod): UIO[Boolean] =
    podApi
      .ping(pod.address)
      .timeout(config.pingTimeout)
      .map(_.exists(_.forall(_ == pod.registration)))
      .orElseSucceed(false)

  private def podAt(states: Map[Role, ShardManagerState], podAddress: PodAddress): Option[PodWithMetadata] =
    states.values.iterator.flatMap(_.pods.get(podAddress)).nextOption()

  private def findPod(podAddress: PodAddress): UIO[Option[PodWithMetadata]] =
    stateRef.get.map(podAt(_, podAddress))

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

  def unregister(podAddress: PodAddress, registration: String): UIO[Unit] =
    unregisterIf(podAddress)(_.pod.registration == registration)

  // check and remove in one update, so that concurrent calls unregister the pod once
  private def unregisterIf(podAddress: PodAddress)(matches: PodWithMetadata => Boolean): UIO[Unit] =
    modifyStates(removePod(_, podAddress, matches))
      .flatMap(
        ZIO.foreachDiscard(_)((pod, shards) => announceUnregistered(pod, shards) *> persistPods.background(scope))
      )
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
      _ <- persistAssignments(pod.role).background(scope).when(unassignments.nonEmpty)
      _ <- rebalance(pod.role, rebalanceImmediately = true).background(scope)
    } yield ()

  private def rebalance(role: Role, rebalanceImmediately: Boolean): UIO[Unit] =
    rebalanceSemaphores(role).flatMap(_.withPermit(rebalanceWithPermit(role, rebalanceImmediately)))

  // a pod may have missed shards recorded on it before the previous Shard Manager stopped
  private def resendAssignments(role: Role): UIO[Unit] =
    rebalanceSemaphores(role).flatMap(
      _.withPermit(
        getAssignments(role).flatMap(assignments =>
          ZIO.foreachParDiscard(assignments.values.flatten.toSet)(podAddress =>
            stateRef.get.flatMap(states =>
              ZIO.foreachDiscard(states.get(role).flatMap(state => state.pods.get(podAddress).map(state -> _))) {
                (state, registered) =>
                  val (shards, registration) = (state.shardsPerPod(podAddress), registered.pod.registration)
                  callPod(role, podAddress, registration)(
                    podApi.assignShards(podAddress, role, registration, shards)
                  ).ignore
                    .when(shards.nonEmpty)
              }
            )
          )
        )
      )
    )

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
      registrations                = state.pods.view.mapValues(_.pod.registration).toMap
      // do the unassignments first
      failedUnassignments         <- unassignShards(role, readyUnassignments, registrations)
      failedUnassignedPods         = failedUnassignments.map(_._1)
      failedUnassignedShards       = failedUnassignments.flatMap(_._2).toSet
      // remove assignments of shards that couldn't be unassigned, as well as faulty pods
      filteredAssignments          = (readyAssignments -- failedUnassignedPods).map { case (podAddress, shards) =>
                                       podAddress -> (shards diff failedUnassignedShards)
                                     }
      // then do the assignments
      failedAssignments           <- assignShards(role, filteredAssignments, registrations)
      failedPods                   = failedPingedPods ++ failedUnassignedPods ++ failedAssignments.map(_._1)
      // check if failing pods are still up
      _                           <- ZIO.foreachDiscard(failedPods)(notifyUnhealthyPod(_)).background(scope).when(failedPods.nonEmpty)
      _                           <- ZIO.logWarning(s"Failed to rebalance pods for role ${role.name}: $failedPods").when(failedPods.nonEmpty)
      // retry rebalancing later if there was any failure
      _                           <- (Clock.sleep(config.rebalanceRetryInterval) *> rebalance(role, rebalanceImmediately))
                                       .background(scope)
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

  // a failed call may have reached the pod, so it's undone; returns the failed moves
  private def unassignShards(
    role: Role,
    moves: Map[PodAddress, Set[ShardId]],
    registrations: Map[PodAddress, String]
  ): UIO[List[(PodAddress, Set[ShardId])]] =
    ZIO.filterNotPar(moves.toList) { (podAddress, shards) =>
      val registration = registrations(podAddress)
      val call         = callPod(role, podAddress, registration)
      ZIO.uninterruptibleMask(restore =>
        restore(call(podApi.unassignShards(podAddress, role, registration, shards))).foldZIO(
          _ => restore(call(podApi.assignShards(podAddress, role, registration, shards))).ignore.as(false),
          _ =>
            setShards(role, shards, None).flatMap(
              ZIO
                .foreach(_)(changed =>
                  eventsHub.publish(ShardingEvent.ShardsUnassigned(podAddress, role, changed)).when(changed.nonEmpty)
                )
                .map(_.isDefined)
            )
        )
      )
    }

  // persisted before the calls, so that shards a pod received stay on it if the Shard Manager stops;
  // a failed call may have reached the pod, so it's undone; returns the failed moves
  private def assignShards(
    role: Role,
    moves: Map[PodAddress, Set[ShardId]],
    registrations: Map[PodAddress, String]
  ): UIO[List[(PodAddress, Set[ShardId])]] =
    for {
      reserved <- ZIO.foreach(moves.toList)((podAddress, shards) =>
                    setShards(role, shards, Some(podAddress -> registrations(podAddress)))
                      .map(podAddress -> _)
                  )
      _        <- persistAssignments(role).when(reserved.exists(_._2.exists(_.nonEmpty)))
      failed   <- ZIO.filterNotPar(reserved) {
                    case (_, None)                          => ZIO.succeed(false)
                    case (podAddress, Some(reservedShards)) =>
                      val shards       = moves(podAddress)
                      val registration = registrations(podAddress)
                      val call         = callPod(role, podAddress, registration)
                      val keep         = setShards(role, shards, Some(podAddress -> registration)).flatMap(
                        ZIO
                          .foreach(_)(changed =>
                            eventsHub
                              .publish(ShardingEvent.ShardsAssigned(podAddress, role, reservedShards ++ changed))
                              .when((reservedShards ++ changed).nonEmpty)
                          )
                          .map(_.isDefined)
                      )
                      ZIO.uninterruptibleMask(restore =>
                        restore(call(podApi.assignShards(podAddress, role, registration, shards))).foldZIO(
                          _ =>
                            restore(call(podApi.unassignShards(podAddress, role, registration, shards)))
                              .foldZIO(_ => keep, _ => releaseShards(role, podAddress, shards))
                              .as(false),
                          _ => keep
                        )
                      )
                  }
    } yield failed.map { case (podAddress, _) => podAddress -> moves(podAddress) }

  // fails once the registration left, so that a hung call doesn't block the role
  private def callPod(role: Role, podAddress: PodAddress, registration: String)(call: Task[Unit]): Task[Unit] = {
    val left              = new RuntimeException(s"Pod $podAddress left")
    def matches(pod: Pod) = pod.address == podAddress && pod.role == role && pod.registration == registration
    ZIO.scoped(
      eventsHub.subscribe.flatMap(events =>
        stateRef.get.flatMap(states =>
          if (states.get(role).exists(_.pods.get(podAddress).exists(registered => matches(registered.pod))))
            call.raceFirst(
              events.take.repeatUntil {
                case ShardingEvent.PodUnregistered(pod) => matches(pod)
                case _                                  => false
              } *> ZIO.fail(left)
            )
          else ZIO.fail(left)
        )
      )
    )
  }

  // returns the shards that changed, or None if the target registration isn't registered
  private def setShards(
    role: Role,
    shards: Set[ShardId],
    target: Option[(PodAddress, String)]
  ): UIO[Option[Set[ShardId]]] =
    modifyStates(states =>
      states
        .get(role)
        .filter(state =>
          target
            .forall((podAddress, registration) => state.pods.get(podAddress).exists(_.pod.registration == registration))
        ) match {
        case Some(state) =>
          val owner   = target.map(_._1)
          val changed = shards.filter(shard => state.shards.get(shard).exists(_ != owner))
          (Some(changed), states.updated(role, state.copy(shards = state.shards ++ changed.map(_ -> owner))))
        case None        => (None, states)
      }
    )

  // only the shards still on the pod
  private def releaseShards(role: Role, podAddress: PodAddress, shards: Set[ShardId]): UIO[Unit] =
    modifyStates(states =>
      (
        (),
        states.get(role).fold(states) { state =>
          val released = shards.filter(shard => state.shards.get(shard).contains(Some(podAddress)))
          states.updated(role, state.copy(shards = state.shards ++ released.map(_ -> None)))
        }
      )
    )
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
        scope               <- ZIO.scope
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
        _                   <- ZIO.addFinalizer(shardManager.persistAll)
        _                   <- shardManager.persistPods
        // rebalance immediately if there are unassigned shards
        _                   <- ZIO.foreachDiscard(initialStates) { case (role, state) =>
                                 (shardManager.resendAssignments(role) *>
                                   shardManager.rebalance(role, rebalanceImmediately = state.unassignedShards.nonEmpty))
                                   .background(scope)
                               }
        // start a regular rebalance at the given interval, per role so that a slow one doesn't delay the others
        _                   <- state.get
                                 .flatMap(states => ZIO.foreachDiscard(states.keySet)(shardManager.rebalanceIfIdle(_).fork))
                                 .repeat(Schedule.spaced(config.rebalanceInterval))
                                 .background(scope)
        _                   <- shardManager.getShardingEvents.mapZIO(event => ZIO.logInfo(event.toString)).runDrain.background(scope)
        _                   <- shardManager.checkAllPodsHealth.repeat(Schedule.spaced(config.podHealthCheckInterval)).background(scope)
        _                   <- ZIO.logInfo("Shard Manager loaded")
      } yield shardManager
    }

  extension [A](effect: UIO[A]) {
    // interrupted when the scope closes, without waiting for an uninterruptible part such as a stuck storage write
    private def background(scope: Scope): UIO[Fiber.Runtime[Nothing, A]] = effect.disconnect.interruptible.forkIn(scope)
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
