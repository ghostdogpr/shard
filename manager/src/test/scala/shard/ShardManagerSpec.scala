package shard

import shard.ShardManager.{ PodWithMetadata, ShardManagerState }
import shard.interfaces.{ Pods, PodsHealth, Storage }
import zio._
import zio.stream.ZStream
import zio.test._

import java.time.OffsetDateTime

object ShardManagerSpec extends ZIOSpecDefault {
  private val role = Role.default
  private val pod1 = PodWithMetadata(Pod(PodAddress("1", 1), "1.0.0", role), OffsetDateTime.MIN)
  private val pod2 = PodWithMetadata(Pod(PodAddress("2", 2), "1.0.0", role), OffsetDateTime.MIN)
  private val pod3 = PodWithMetadata(Pod(PodAddress("3", 3), "1.0.0", role), OffsetDateTime.MIN)

  override def spec: Spec[Any, Throwable] =
    suite("ShardManagerSpec")(
      suite("Unit tests")(
        test("Rebalance unbalanced assignments") {
          val state                        =
            ShardManagerState(
              pods = Map(pod1.pod.address -> pod1, pod2.pod.address -> pod2),
              shards = Map(1 -> Some(pod1.pod.address), 2 -> Some(pod1.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(
            assignments.contains(pod2.pod.address),
            assignments.size == 1,
            unassignments.contains(pod1.pod.address),
            unassignments.size == 1
          )
        },
        test("Don't rebalance to pod with older version") {
          val state                        =
            ShardManagerState(
              pods = Map(
                pod1.pod.address -> pod1,
                pod2.pod.address -> pod2.copy(pod = pod2.pod.copy(version = "0.1.2"))
              ), // older version
              shards = Map(1 -> Some(pod1.pod.address), 2 -> Some(pod1.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(assignments.isEmpty, unassignments.isEmpty)
        },
        test("Don't rebalance when already well balanced") {
          val state                        =
            ShardManagerState(
              pods = Map(pod1.pod.address -> pod1, pod2.pod.address -> pod2),
              shards = Map(1 -> Some(pod1.pod.address), 2 -> Some(pod2.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(assignments.isEmpty, unassignments.isEmpty)
        },
        test("Don't rebalance when only 1 shard difference") {
          val state                        =
            ShardManagerState(
              pods = Map(pod1.pod.address -> pod1, pod2.pod.address -> pod2),
              shards = Map(1 -> Some(pod1.pod.address), 2 -> Some(pod1.pod.address), 3 -> Some(pod2.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(assignments.isEmpty, unassignments.isEmpty)
        },
        test("Rebalance when 2 shard difference") {
          val state                        =
            ShardManagerState(
              pods = Map(pod1.pod.address -> pod1, pod2.pod.address -> pod2),
              shards = Map(
                1 -> Some(pod1.pod.address),
                2 -> Some(pod1.pod.address),
                3 -> Some(pod1.pod.address),
                4 -> Some(pod2.pod.address)
              )
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(
            assignments.contains(pod2.pod.address),
            assignments.size == 1,
            unassignments.contains(pod1.pod.address),
            unassignments.size == 1
          )
        },
        test("Pick the pod with less shards") {
          val state                        =
            ShardManagerState(
              pods = Map(pod1.pod.address -> pod1, pod2.pod.address -> pod2, pod3.pod.address -> pod3),
              shards = Map(1 -> Some(pod1.pod.address), 2 -> Some(pod1.pod.address), 3 -> Some(pod2.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(
            assignments.contains(pod3.pod.address),
            assignments.size == 1,
            unassignments.contains(pod1.pod.address),
            unassignments.size == 1
          )
        },
        test("Don't rebalance if pod list is empty") {
          val state                        =
            ShardManagerState(
              pods = Map(),
              shards = Map(1 -> Some(pod1.pod.address))
            )
          val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(state, 1d)
          assertTrue(assignments.isEmpty, unassignments.isEmpty)
        },
        test("Balance well when 30 nodes are starting one by one") {
          val state =
            ShardManagerState(
              pods = Map(),
              shards = (1 to 300).map(_ -> None).toMap
            )

          val result =
            (1 to 30).foldLeft(state) { case (state, podNumber) =>
              val podAddress                   = PodAddress("", podNumber)
              val s1                           = state.copy(pods =
                state.pods.updated(podAddress, PodWithMetadata(Pod(podAddress, "v1", role), OffsetDateTime.now()))
              )
              val (assignments, unassignments) = ShardManager.decideAssignmentsForUnbalancedShards(s1, 1d)
              val s2                           = unassignments.foldLeft(s1) { case (state, (_, shards)) =>
                shards.foldLeft(state) { case (state, shard) =>
                  state.copy(shards = state.shards.updated(shard, None))
                }
              }
              val s3                           = assignments.foldLeft(s2) { case (state, (address, shards)) =>
                shards.foldLeft(state) { case (state, shard) =>
                  state.copy(shards = state.shards.updated(shard, Some(address)))
                }
              }
              s3
            }

          val shardsPerPod =
            result.shards.groupBy(_._2).collect { case (Some(address), shards) => address -> shards.keySet }
          assertTrue(shardsPerPod.values.forall(_.size == 10))
        }
      ),
      suite("Simulations")(
        test("Simulate scaling out scenario") {
          (for {
            // a pod of another role whose rebalance never ends must not hold back this role
            // (registered twice: a re-register counts the pod once)
            _           <- simulate(List(SimulationEvent.PodRegister(stuckPod), SimulationEvent.PodRegister(stuckPod)))
            // setup 20 pods first
            _           <- simulate(
                             (1 to 20).toList.map(i => SimulationEvent.PodRegister(Pod(PodAddress("server", i), "1", role)))
                           )
            _           <- TestClock.adjust(10 minutes)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            stuckShards <- ManagerMetrics.assignedShards
                             .tagged("role", stuckPod.role.name)
                             .tagged("pod_address", stuckPod.address.toString)
                             .value
            stuckCount  <- ManagerMetrics.pods.tagged("role", stuckPod.role.name).value
            // check that all pods are assigned and that all pods have 15 shards each
            assert1      = assertTrue(
                             assignments.values.forall(_.isDefined),
                             assignments.groupBy(_._2).forall(_._2.size == 15),
                             stuckShards.value == ManagerConfig.default.numberOfShards(stuckPod.role).toDouble,
                             stuckCount.value == 1
                           )

            // bring 5 new pods
            _           <- simulate(
                             (21 to 25).toList.map(i => SimulationEvent.PodRegister(Pod(PodAddress("server", i), "1", role)))
                           )
            _           <- TestClock.adjust(20 seconds)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            // check that new pods received some shards but only 6
            assert2      = assertTrue(assignments.groupBy(_._2).filter(_._1.exists(_.port > 20)).forall(_._2.size == 6))

            _           <- TestClock.adjust(1 minute)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            // check that all pods now have 12 shards each
            assert3      = assertTrue(assignments.groupBy(_._2).forall(_._2.size == 12))

          } yield assert1 && assert2 && assert3).provide(
            ZLayer.make[ShardManager](config, storage, stuckPods, PodsHealth.local, ShardManager.live)
          )
        },
        test("Shards assigned by a call that failed after reaching the pod are not given to another pod") {
          for {
            undone      <- lostAssignReply(undo = ZIO.unit)
            recorded    <- lostAssignReply(undo = ZIO.fail(new Exception("unreachable")))
            interrupted <- lostAssignReply(undo = ZIO.never)
          } yield assertTrue(
            undone._1 == undone._2,
            undone._1.get(pod1.pod.address).exists(_.size == 150),
            recorded._1 == recorded._2,
            recorded._1.get(pod1.pod.address).exists(_.size == 300),
            interrupted._1 == interrupted._2,
            interrupted._1.get(pod1.pod.address).exists(_.size == 300)
          )
        },
        test("Simulate scaling down scenario") {
          (for {
            // setup 25 pods first
            _           <- simulate(
                             (1 to 25).toList.map(i => SimulationEvent.PodRegister(Pod(PodAddress("server", i), "1", role)))
                           )
            _           <- TestClock.adjust(10 minutes)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            // check that all pods are assigned and that all pods have 12 shards each
            assert1      = assertTrue(
                             assignments.values.forall(_.isDefined),
                             assignments.groupBy(_._2).forall(_._2.size == 12)
                           )

            // remove 5 pods
            _           <- simulate((21 to 25).toList.map(i => SimulationEvent.PodUnregister(PodAddress("server", i))))
            _           <- TestClock.adjust(1 second)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            // check that all shards have been rebalanced already
            assert2      = assertTrue(
                             assignments.values.forall(_.exists(_.port <= 20)),
                             assignments.groupBy(_._2).forall(_._2.size == 15)
                           )

            // a pod of another role at the address of pod 1 replaces it
            replacing    = Pod(PodAddress("server", 1), "1", Role("replacing"))
            _           <- simulate(List(SimulationEvent.PodRegister(replacing)))
            _           <- TestClock.adjust(1 second)
            assignments <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(role))
            replaced    <- ZIO.serviceWithZIO[ShardManager](_.getAssignments(replacing.role))
            assert3      = assertTrue(
                             assignments.values.forall(_.exists(address => address.port > 1 && address.port <= 20)),
                             replaced.values.forall(_.contains(replacing.address))
                           )

          } yield assert1 && assert2 && assert3).provide(shardManager)
        },
        test("Simulate temporary storage restart followed by manager restart") {
          {
            val setup = (for {
              _ <- simulate {
                     (1 to 10).toList.map(i => SimulationEvent.PodRegister(Pod(PodAddress("server", i), "1", role)))
                   }
              _ <- TestClock.adjust(10 minutes)
              // busy wait for the forked daemon fibers to do their job
              _ <- ZIO.iterate(Map.empty[ShardId, Option[PodAddress]])(_.isEmpty)(_ =>
                     ZIO.serviceWithZIO[Storage](_.getAssignments(role))
                   )
              _ <- ZIO.iterate(Set.empty[Pod])(_.isEmpty)(_ => ZIO.serviceWithZIO[Storage](_.getPods))
              // simulate non-persistent storage restart
              _ <- ZIO.serviceWithZIO[Storage](s => s.saveAssignments(role, Map.empty) *> s.savePods(Set.empty))
            } yield {}).provideSome[Storage](
              ZLayer.makeSome[Storage, ShardManager](config, Pods.noop, PodsHealth.local, ShardManager.live)
            )

            val test = for {
              shutdownAssignments <- ZIO.serviceWithZIO[Storage](_.getAssignments(role))
              shutdownPods        <- ZIO.serviceWithZIO[Storage](_.getPods)
            } yield
              // manager should have saved its state to storage when it shut down
              assertTrue(shutdownAssignments.nonEmpty && shutdownPods.nonEmpty)

            setup *> test
          }.provide(Storage.memory)
        },
        test("Recover stored assignments by role") {
          val role1   = Role("recovered-1")
          val pod1    = Pod(PodAddress("server", 1), "1", role1)
          val pod2    = Pod(PodAddress("server", 2), "1", Role("recovered-2"))
          val deadPod = Pod(PodAddress("server", 3), "1", Role("recovered-3"))
          val pod4    = Pod(PodAddress("server", 4), "1", Role("recovered-4"))
          val manager = ZIO.serviceWithZIO[ShardManager]
          for {
            checks    <- Ref.make(0)
            podsHealth = ZLayer.succeed(new PodsHealth {
                           def isAlive(pod: Pod): UIO[Boolean] = checks.update(_ + 1).as(pod != deadPod)
                         })
            blocked   <- Ref.make(false)
            closing   <- Ref.make(false)
            shutdown  <- Ref.make(Set.empty[Role])
            // the first write of pod4's assignments never returns, even interrupted, which must not block the others,
            // role1's write on shutdown fails, which must not keep the other roles from being written,
            // and the pods write on shutdown never returns, even interrupted, which must not hold back the release
            storage    = Storage.memory >>> ZLayer.fromFunction((memory: Storage) =>
                           new Storage {
                             export memory.{ assignmentsStream, getAssignments, getPods }
                             def savePods(pods: Set[Pod]): Task[Unit]                                                   =
                               ZIO.ifZIO(closing.get)(ZIO.never.uninterruptible, memory.savePods(pods))
                             def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] =
                               closing.get.flatMap {
                                 case true if role == role1 => ZIO.dieMessage("storage down")
                                 case true                  => shutdown.update(_ + role) *> memory.saveAssignments(role, assignments)
                                 case false                 =>
                                   blocked
                                     .modify(done => (role == pod4.role && !done, done || role == pod4.role))
                                     .flatMap(block =>
                                       if (block) ZIO.never.uninterruptible else memory.saveAssignments(role, assignments)
                                     )
                               }
                           }
                         )
            run       <- (for {
                           _      <- ZIO.serviceWithZIO[Storage](s =>
                                       s.savePods(Set(pod1, pod2, deadPod)) *>
                                         // a stale assignment to a pod of another role
                                         s.saveAssignments(role1, Map(1 -> Some(pod2.address))) *>
                                         s.saveAssignments(deadPod.role, Map(1 -> Some(deadPod.address)))
                                     )
                           result <- (for {
                                       _           <- TestClock.adjust(1 minute)
                                       _           <- manager(_.register(pod4))
                                       assignments <- manager(_.getAssignments(role1))
                                       // a role whose stored pods are all dead still has its shards, all unassigned
                                       deadRole    <- manager(_.getAssignments(deadPod.role))
                                       // the last pod of a role leaving clears its assignments in storage
                                       _           <- manager(_.unregister(pod1.address))
                                       stored      <- Live.live(
                                                        ZIO
                                                          .serviceWithZIO[Storage](_.getAssignments(role1))
                                                          .repeat(
                                                            Schedule.recurUntil[Map[ShardId, Option[PodAddress]]](stored =>
                                                              stored.size == ManagerConfig.default.numberOfShards(role1) &&
                                                                !stored.values.exists(_.contains(pod1.address))
                                                            ) <* Schedule.spaced(10.millis)
                                                          )
                                                          .timeoutFail(new Exception("assignments not persisted"))(5.seconds)
                                                      )
                                       _           <- closing.set(true)
                                     } yield assertTrue(
                                       assignments.get(1).flatten.contains(pod1.address),
                                       deadRole.size == ManagerConfig.default.numberOfShards(deadPod.role),
                                       deadRole.values.forall(_.isEmpty),
                                       stored.size == ManagerConfig.default.numberOfShards(role1),
                                       !stored.values.exists(_.contains(pod1.address))
                                     )).provideSome[Storage](config, Pods.noop, podsHealth, ShardManager.live)
                         } yield result).provide(storage).forkDaemon
            // the release waits for the shutdown timeouts
            _         <- (ZIO.whenZIO(closing.get)(TestClock.adjust(1.second)) *> Live.live(ZIO.sleep(10.millis))).forever.fork
            result    <- Live.live(run.join.timeoutFail(new Exception("release not finished"))(10.seconds))
            persisted <- shutdown.get
            // the health checks stopped with the Shard Manager
            released  <- checks.get
            _         <- Live.live(ZIO.sleep(1.second))
            after     <- checks.get
          } yield result && assertTrue(persisted == Set(pod2.role, deadPod.role), after == released)
        }
      )
    )

  sealed trait SimulationEvent
  object SimulationEvent {
    case class PodRegister(pod: Pod)                 extends SimulationEvent
    case class PodUnregister(podAddress: PodAddress) extends SimulationEvent
  }

  val config: ULayer[ManagerConfig] = ZLayer.succeed(ManagerConfig.default)

  private val stuckPod = Pod(PodAddress("stuck", 1), "1", Role("stuck"))

  // assigning shards to stuckPod never completes
  val stuckPods: ULayer[Pods] = Pods.noop.project(noop =>
    new Pods {
      export noop.{ ping, sendMessage, sendStream, sendStreamAndReceiveStream, unassignShards }
      def assignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit] =
        if (pod == stuckPod.address) ZIO.never else noop.assignShards(pod, role, shards)
    }
  )

  val storage: ULayer[Storage] = ZLayer.succeed(new Storage {
    def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]                       = ZIO.succeed(Map.empty)
    def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit]   = ZIO.unit
    def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] = ZStream.empty
    def getPods: Task[Set[Pod]]                                                                  = ZIO.succeed(Set.empty)
    def savePods(pods: Set[Pod]): Task[Unit]                                                     = ZIO.unit
  })

  val shardManager: ZLayer[Any, Throwable, ShardManager] =
    ZLayer.make[ShardManager](config, storage, Pods.noop, PodsHealth.local, ShardManager.live)

  // pod1 applies its first assign but the reply is lost, and the unassign undoing it runs `undo` first, then pod2
  // registers and the Shard Manager stops; returns the shards persisted on each pod and those each pod holds
  private def lostAssignReply(undo: Task[Unit]): Task[(Map[PodAddress, Set[ShardId]], Map[PodAddress, Set[ShardId]])] =
    for {
      hosted    <- Ref.make(Map.empty[PodAddress, Set[ShardId]])
      lost      <- Ref.make(true)
      firstUndo <- Ref.make(Option(undo))
      persisted <- Ref.make(Map.empty[ShardId, Option[PodAddress]])
      pods       = Pods.noop.project(noop =>
                     new Pods {
                       export noop.{ ping, sendMessage, sendStream, sendStreamAndReceiveStream }
                       def assignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit]   =
                         hosted.update(m => m.updated(pod, m.getOrElse(pod, Set.empty) ++ shards)) *>
                           ZIO.whenZIO(lost.getAndSet(false))(ZIO.fail(new Exception("reply lost"))).unit
                       def unassignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit] =
                         firstUndo.getAndSet(None).flatMap(ZIO.foreachDiscard(_)(identity)) *>
                           hosted.update(m => m.updated(pod, m.getOrElse(pod, Set.empty) -- shards))
                     }
                   )
      saving     = storage.project(base =>
                     new Storage {
                       export base.{ assignmentsStream, getAssignments, getPods, savePods }
                       def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] =
                         persisted.set(assignments)
                     }
                   )
      _         <- (simulate(List(SimulationEvent.PodRegister(pod1.pod))) *>
                     TestClock.adjust(1.second) *>
                     simulate(List(SimulationEvent.PodRegister(pod2.pod))) *>
                     TestClock.adjust(1.second))
                     .provide(ZLayer.make[ShardManager](config, saving, pods, PodsHealth.local, ShardManager.live))
      saved     <- persisted.get
      held      <- hosted.get
    } yield (
      saved.toList.collect { case (shard, Some(pod)) => pod -> shard }
        .groupMap(_._1)(_._2)
        .view
        .mapValues(_.toSet)
        .toMap,
      held.filter(_._2.nonEmpty)
    )

  def simulate(events: List[SimulationEvent]): RIO[ShardManager, Unit] =
    for {
      shardManager <- ZIO.service[ShardManager]
      _            <- ZIO.foreachDiscard(events) {
                        case SimulationEvent.PodRegister(pod)          => shardManager.register(pod)
                        case SimulationEvent.PodUnregister(podAddress) => shardManager.unregister(podAddress)
                      }
    } yield ()
}
