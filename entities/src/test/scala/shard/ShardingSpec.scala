package shard

import shard.Messenger.MessengerTimeout
import shard.CounterActor.CounterMessage._
import shard.CounterActor._
import shard.WorkerActor.WorkerMessage._
import shard.WorkerActor._
import shard.errors._
import shard.interfaces.{ Pods, ShardManagerClient, Storage }
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.internal.LocalShardManagerClient
import shard.javaSerialization.given
import zio.stream.{ SubscriptionRef, ZChannel, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object ShardingSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ShardingSpec")(
      suite("local-pod shortcut")(tests*).provideSomeShared[Scope](
        LocalSharding.live,
        LocalShardManagerClient.live,
        Storage.memory,
        ZLayer.succeed(Config.default)
      ),
      suite("simulate remote pods (forces serialization on every hop)")(tests*).provideSomeShared[Scope](
        LocalSharding.live,
        LocalShardManagerClient.live,
        Storage.memory,
        ZLayer.succeed(Config.default.copy(simulateRemotePods = true))
      ),
      suite("interceptor in Config")(defaultInterceptor).provideSomeShared[Scope](
        LocalSharding.live,
        LocalShardManagerClient.live,
        Storage.memory,
        ZLayer.succeed(Config.default.copy(entityInterceptor = logging))
      ),
      suite("registered pods")(
        test("Registered pods are read from the storage") {
          val pod1 = Pod(PodAddress("host1", 1), "1.0", Role.default)
          val pod2 = Pod(PodAddress("host2", 2), "2.0", Role("other"))
          for {
            _    <- ZIO.serviceWithZIO[Storage](_.savePods(Set(pod1, pod2)))
            pods <- Sharding.getRegisteredPods
          } yield assertTrue(pods == Set(pod1, pod2))
        }
      ).provide(
        LocalSharding.live,
        LocalShardManagerClient.live,
        Storage.memory,
        ZLayer.succeed(Config.default)
      ),
      suite("shards taken back by the Shard Manager")(
        test("A registered pod keeps its shards until they are unassigned") {
          for {
            _      <- Sharding.registerScoped
            config <- ZIO.service[Config]
            _      <- giveAllShardsTo(otherPod)
            _      <- Clock.sleep(300.millis)
            pods   <- Sharding.getPods
          } yield assertTrue(pods == Set(PodAddress(config.selfHost, config.shardingPort)))
        },
        test("A pod stops its singletons when it unregisters, and sends messages to the pod that took its shards") {
          for {
            started <- Promise.make[Nothing, Unit]
            stopped <- Promise.make[Nothing, Unit]
            _       <- Sharding.registerEntity(Counter)(behavior)
            _       <- Sharding.registerScoped
            _       <- Sharding
                         .registerSingleton("singleton", (started.succeed(()) *> ZIO.never).onInterrupt(stopped.succeed(())))
                         .uninterruptible *> started.await
            counter <- Sharding.messenger(Counter, MessengerTimeout.Timeout(5.seconds))
            _       <- counter.send("c1")(IncrementCounter())        // handled locally
            _       <- Sharding.unregister
            done    <- stopped.isDone
            // the Shard Manager gives the shards of the pod to another pod, without unassigning them from it
            _       <- giveAllShardsTo(otherPod)
            _       <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_ == Set(otherPod))
            _       <- counter.sendDiscard("c1")(IncrementCounter()) // sent to the other pod
          } yield assertTrue(done)
        },
        test("A message expecting a reply from another pod fails with Pods.noop") {
          for {
            _       <- Sharding.registerScoped
            _       <- Sharding.unregister
            _       <- giveAllShardsTo(otherPod)
            _       <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_ == Set(otherPod))
            counter <- Sharding.messenger(Counter, MessengerTimeout.Timeout(5.seconds))
            result  <- counter.send("c1")(GetCounter()).either
          } yield assertTrue(result == Left(EntityDefect("Pods.noop sends no messages")))
        }
      ).provideSome[Scope](
        Sharding.live,
        Pods.noop, // messages sent to other pods without expecting a reply succeed
        LocalShardManagerClient.live,
        Storage.memory,
        ZLayer.succeed(Config.default)
      ),
      test("A pod that unregistered stops its singletons when it reconnects to the storage") {
        val config = Config.default.copy(refreshAssignmentsRetryInterval = 10.millis)
        val self   = PodAddress(config.selfHost, config.shardingPort)
        for {
          assignments <- Ref.make(Map(1 -> Option(self)))
          disconnect  <- Promise.make[Nothing, Unit]
          stopped     <- Promise.make[Nothing, Unit]
          reconnected <- Promise.make[Nothing, Unit]
          storage     <- reconnectingStorage(assignments, disconnect, reconnected)
          _           <- (Sharding.registerSingleton("singleton", ZIO.never.onInterrupt(stopped.succeed(()))) *>
                           Sharding.register *>
                           Sharding.unregister *>
                           disconnect.succeed(()) *>
                           stopped.await).provide(
                           Sharding.live,
                           Pods.noop,
                           ZLayer.succeed(takingShardsBack(assignments)),
                           ZLayer.succeed(storage),
                           ZLayer.succeed(config)
                         )
        } yield assertCompletes
      },
      test(
        "A singleton slow to stop doesn't block the unregister or the assignments of a pod reconnecting to the storage"
      ) {
        val config =
          Config.default.copy(refreshAssignmentsRetryInterval = 10.millis, entityTerminationTimeout = 100.millis)
        val self   = PodAddress(config.selfHost, config.shardingPort)
        for {
          assignments <- Ref.make(Map(1 -> Option(self)))
          disconnect  <- Promise.make[Nothing, Unit]
          release     <- Promise.make[Nothing, Unit]
          reconnected <- Promise.make[Nothing, Unit]
          storage     <- reconnectingStorage(assignments, disconnect, reconnected)
          result      <- (Sharding.registerSingleton("singleton", ZIO.never.onInterrupt(release.await)) *>
                           Sharding.register *>
                           Sharding.unregister *>
                           disconnect.succeed(()) *>
                           reconnected.await.timeout(5.seconds) <*
                           release.succeed(())).provide(
                           Sharding.live,
                           Pods.noop,
                           ZLayer.succeed(takingShardsBack(assignments)),
                           ZLayer.succeed(storage),
                           ZLayer.succeed(config)
                         )
        } yield assertTrue(result.isDefined)
      },
      test("A snapshot fetched before the Shard Manager assigned shards to the pod doesn't undo the assignment") {
        val config = Config.default.copy(refreshAssignmentsRetryInterval = 10.millis)
        val self   = PodAddress(config.selfHost, config.shardingPort)
        for {
          assignments <- Ref.make(Map[ShardId, Option[PodAddress]](1 -> None))
          fetches     <- Ref.make(0)
          disconnect  <- Promise.make[Nothing, Unit]
          fetched     <- Promise.make[Nothing, Unit]
          late        <- Promise.make[Nothing, Unit]
          reconnected <- Promise.make[Nothing, Unit]
          stopped     <- Promise.make[Nothing, Unit]
          manager      = new ShardManagerClient {
                           def register(podAddress: PodAddress, role: Role): Task[Unit]           = ZIO.unit
                           def unregister(podAddress: PodAddress): Task[Unit]                     = ZIO.unit
                           def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit]             = ZIO.unit
                           // the reply to the first call after reconnecting arrives late, with the old assignments
                           def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] =
                             assignments.get <* fetches
                               .updateAndGet(_ + 1)
                               .flatMap(n => (fetched.succeed(()) *> late.await).when(n == 2))
                         }
          storage     <- reconnectingStorage(assignments, disconnect, reconnected)
          result      <- (for {
                           sharding  <- ZIO.service[Sharding]
                           _         <- Sharding.registerSingleton("singleton", ZIO.never.onInterrupt(stopped.succeed(())))
                           _         <- disconnect.succeed(())
                           _         <- fetched.await
                           _         <- assignments.set(Map(1 -> Some(self)))
                           _         <- sharding.assign(Set(1))
                           _         <- late.succeed(())
                           _         <- reconnected.await
                           singleton <- Sharding.isSingletonNode
                           interrupt <- stopped.isDone
                         } yield (singleton, interrupt)).provide(
                           Sharding.live,
                           Pods.noop,
                           ZLayer.succeed(manager),
                           ZLayer.succeed(storage),
                           ZLayer.succeed(config)
                         )
        } yield assertTrue(result == (true, false))
      },
      test("A snapshot fetched before the Shard Manager recorded a move doesn't undo it") {
        val config = Config.default.copy(refreshAssignmentsRetryInterval = 10.millis)
        val self   = PodAddress(config.selfHost, config.shardingPort)
        for {
          assignments <- Ref.make(Map[ShardId, Option[PodAddress]](1 -> Some(self), 2 -> None))
          disconnect  <- Promise.make[Nothing, Unit]
          reconnected <- Promise.make[Nothing, Unit]
          storage     <- reconnectingStorage(assignments, disconnect, reconnected)
          own         <- (for {
                           sharding <- ZIO.service[Sharding]
                           _        <- sharding.unassign(Set(1))
                           _        <- sharding.assign(Set(2))
                           _        <- disconnect.succeed(())
                           _        <- reconnected.await
                           _        <- assignments.set(Map(1 -> Some(otherPod), 2 -> Some(self)))
                           own      <- sharding.thisPodAssignments
                         } yield own).provide(
                           Sharding.live,
                           Pods.noop,
                           ZLayer.succeed(takingShardsBack(assignments)),
                           ZLayer.succeed(storage),
                           ZLayer.succeed(config)
                         )
        } yield assertTrue(own == Chunk(2))
      },
      test("A duplex whose inputs were read is not sent again when its pod becomes unavailable") {
        for {
          attempts <- Ref.make(0)
          snapshot <- Ref.make(Map.empty[ShardId, Option[PodAddress]])
          // reads the request and the first input, then fails like an unreachable pod
          pods      = stubPods(
                        onSend = pod => ZIO.fail(PodUnavailable(pod)),
                        onStream = (pod, messages) =>
                          ZStream.fromZIO(attempts.update(_ + 1) *> messages.take(2).runDrain) *>
                            ZStream.fail(PodUnavailable(pod))
                      )
          result   <- (giveAllShardsTo(otherPod) *>
                        (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_ == Set(otherPod)) *>
                        Sharding.messenger(Counter).flatMap(_.duplex("c1")(Doubled(), ZStream(1, 2, 3)).runDrain.flip))
                        .timeout(5.seconds)
                        .provide(
                          Sharding.live,
                          ZLayer.succeed(pods),
                          ZLayer.succeed(takingShardsBack(snapshot)),
                          Storage.memory,
                          ZLayer.succeed(Config.default)
                        )
          sent     <- attempts.get
        } yield assertTrue(result.contains(StreamCancelled), sent == 1)
      },
      test("A pod reads the assignments from the storage again when its messages can't reach their entity") {
        val config                       = Config.default.copy(refreshAssignmentsRetryInterval = 10.millis)
        val third                        = PodAddress("third", 1)
        def all(pod: Option[PodAddress]) = (1 to config.numberOfShards).map(_ -> pod).toMap
        for {
          snapshot    <- Ref.make(all(Some(otherPod)))
          assignments <- Ref.make(all(None))
          // never completed: the subscription stays open but misses the changes
          disconnect  <- Promise.make[Nothing, Unit]
          reconnected <- Promise.make[Nothing, Unit]
          storage     <- reconnectingStorage(assignments, disconnect, reconnected)
          result      <- (for {
                           send   <- Sharding.messenger(Counter).flatMap(_.sendDiscard("c1")(IncrementCounter())).fork
                           // refused by the other pod, then the storage has no owner for the shard
                           _      <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_.isEmpty)
                           _      <- assignments.set(all(Some(third)))
                           result <- send.join.either
                         } yield result)
                           .timeout(5.seconds)
                           .provide(
                             Sharding.live,
                             ZLayer.succeed(refusingPods),
                             ZLayer.succeed(takingShardsBack(snapshot)),
                             ZLayer.succeed(storage),
                             ZLayer.succeed(config)
                           )
        } yield assertTrue(result.exists(_.isRight))
      },
      test("Assignments read again from the storage don't undo a newer notification") {
        val config                       = Config.default.copy(refreshAssignmentsRetryInterval = 10.millis)
        val third                        = PodAddress("third", 1)
        def all(pod: Option[PodAddress]) = (1 to config.numberOfShards).map(_ -> pod).toMap
        for {
          snapshot      <- Ref.make(all(Some(otherPod)))
          notifications <- Queue.unbounded[Map[ShardId, Option[PodAddress]]]
          reading       <- Promise.make[Nothing, Unit]
          read          <- Promise.make[Nothing, Unit]
          // a read that returns an older state, after a notification
          storage        = new Storage {
                             def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]               =
                               reading.succeed(()) *> read.await.as(all(None))
                             def saveAssignments(role: Role, value: Map[ShardId, Option[PodAddress]]): Task[Unit] = ZIO.unit
                             def assignmentsStream(role: Role)                                                    =
                               ZStream.fromQueue(notifications)
                             def getPods: Task[Set[Pod]]                                                          = ZIO.succeed(Set.empty)
                             def savePods(pods: Set[Pod]): Task[Unit]                                             = ZIO.unit
                           }
          pods          <- (for {
                             send <- Sharding.messenger(Counter).flatMap(_.sendDiscard("c1")(IncrementCounter())).fork
                             _    <- reading.await
                             _    <- notifications.offer(all(Some(third)))
                             _    <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_ == Set(third))
                             _    <- read.succeed(())
                             _    <- send.join
                             _    <- Clock.sleep(200.millis)
                             pods <- Sharding.getPods
                           } yield pods)
                             .timeout(5.seconds)
                             .provide(
                               Sharding.live,
                               ZLayer.succeed(refusingPods),
                               ZLayer.succeed(takingShardsBack(snapshot)),
                               ZLayer.succeed(storage),
                               ZLayer.succeed(config)
                             )
        } yield assertTrue(pods.contains(Set(third)))
      },
      test("Assigning shards that a pod already has doesn't change its shards gauge") {
        // a role of its own, since the gauge is shared by all the pods of a role in this JVM
        val config = Config.default.copy(role = Role("gauge"))
        (for {
          sharding <- ZIO.service[Sharding]
          _        <- sharding.assign(Set(1, 2))
          _        <- sharding.unassign(Set(3))
          _        <- sharding.assign(Set(3))
          shards   <- Metrics.shards.tagged("role", "gauge").value
        } yield assertTrue(shards.value == 300.0)).provide(
          LocalSharding.live,
          LocalShardManagerClient.live,
          Storage.memory,
          ZLayer.succeed(config)
        )
      }
    ) @@ sequential @@ withLiveClock @@ TestAspect.timeout(60.seconds)

  val otherPod: PodAddress = PodAddress("other", 1)

  // a Shard Manager that gives the shards of a pod that unregisters to another pod
  private def takingShardsBack(assignments: Ref[Map[ShardId, Option[PodAddress]]]): ShardManagerClient =
    new ShardManagerClient {
      def register(podAddress: PodAddress, role: Role): Task[Unit]           = ZIO.unit
      def unregister(podAddress: PodAddress): Task[Unit]                     = assignments.set(Map(1 -> Some(otherPod)))
      def notifyUnhealthyPod(podAddress: PodAddress): Task[Unit]             = ZIO.unit
      def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] = assignments.get
    }

  // a storage whose first subscription fails when `disconnect` completes, so that the new assignments are only received
  // from the Shard Manager when reconnecting; `reconnected` completes on the next subscription
  private def reconnectingStorage(
    assignments: Ref[Map[ShardId, Option[PodAddress]]],
    disconnect: Promise[Nothing, Unit],
    reconnected: Promise[Nothing, Unit]
  ): UIO[Storage] =
    Ref.make(false).map { subscribed =>
      new Storage {
        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]]                       =
          assignments.get
        def saveAssignments(role: Role, value: Map[ShardId, Option[PodAddress]]): Task[Unit]         =
          assignments.set(value)
        def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          ZStream.unwrap(subscribed.getAndSet(true).map {
            case false => ZStream.fromZIO(disconnect.await *> ZIO.fail(new Exception("disconnected")))
            case true  => ZStream.fromZIO(reconnected.succeed(())) *> ZStream.never
          })
        def getPods: Task[Set[Pod]]                                                                  = ZIO.succeed(Set.empty)
        def savePods(pods: Set[Pod]): Task[Unit]                                                     = ZIO.unit
      }
    }

  // the other pod refuses messages, any other pod accepts them
  private val refusingPods: Pods =
    stubPods(
      onSend = pod =>
        ZIO.succeed(
          if (pod == otherPod) BinaryReply.EntityNotManagedByThisPod else BinaryReply.Success(Array.emptyByteArray)
        ),
      onStream = (pod, _) => ZStream.fail(PodUnavailable(pod))
    )

  private def stubPods(
    onSend: PodAddress => Task[BinaryReply],
    onStream: (PodAddress, ZStream[Any, Nothing, BinaryMessage]) => ZStream[Any, Throwable, BinaryReply]
  ): Pods =
    new Pods {
      def assignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit]                    = ZIO.unit
      def unassignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit]                  = ZIO.unit
      def ping(pod: PodAddress): Task[Unit]                                                              = ZIO.unit
      def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply]                        = onSend(pod)
      def sendStream(pod: PodAddress, messages: ZStream[Any, Nothing, BinaryMessage]): Task[BinaryReply] =
        ZIO.fail(PodUnavailable(pod))
      def sendStreamAndReceiveStream(
        pod: PodAddress,
        messages: ZStream[Any, Nothing, BinaryMessage]
      ): ZStream[Any, Throwable, BinaryReply]                                                            =
        onStream(pod, messages)
    }

  // what the Shard Manager does when it moves shards
  def giveAllShardsTo(pod: PodAddress): RIO[Storage & Config, Unit] =
    ZIO.serviceWithZIO[Config](config =>
      ZIO.serviceWithZIO[Storage](
        _.saveAssignments(config.role, (1 to config.numberOfShards).map(_ -> Some(pod)).toMap)
      )
    )

  // tests relying on the in-process transport's chunking, delivery order and synchronous cancellation
  val inProcess: String = "in-process"

  val tests: List[Spec[Sharding & Scope, Any]] =
    List(
      test("Send message to entities") {
        for {
          counter <- startCounter()
          _       <- counter.sendDiscard("c1")(IncrementCounter())
          _       <- counter.sendDiscard("c1")(DecrementCounter())
          _       <- counter.sendDiscard("c1")(IncrementCounter())
          _       <- counter.send("c1")(IncrementCounter())
          _       <- counter.send("c2")(IncrementCounter())
          c1      <- counter.send("c1")(GetCounter())
          c2      <- counter.send("c2")(GetCounter())
        } yield assertTrue(c1 == 2, c2 == 1)
      },
      test("Typed failure") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(IncrementCounter())
          error   <- counter.send("c1")(Fail()).flip
          c1      <- counter.send("c1")(GetCounter()) // the entity is still running
          logged  <- unhandledFailures
        } yield assertTrue(error == CounterError("failed"), c1 == 1, logged.isEmpty)
      },
      test("Defect stops the entity") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(IncrementCounter())
          error   <- counter.send("c1")(Die()).flip
          c1      <- counter.send("c1")(GetCounter()) // counter should be restarted
        } yield assertTrue(error == EntityDefect("Entity died with java.lang.Exception: boom"), c1 == 0)
      },
      test("A message that both fails and dies is a defect") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(IncrementCounter())
          error   <- counter.send("c1")(FailAndDie()).flip
          c1      <- counter.send("c1")(GetCounter()) // counter should be restarted
        } yield assertTrue(error == EntityDefect("Entity died with java.lang.Exception: boom"), c1 == 0)
      },
      test("A defect in a response stream ends it with EntityDefect, after its elements") {
        for {
          counter <- startCounter()
          items   <- counter.stream("c1")(DyingStream()).either.runCollect
        } yield assertTrue(
          items == Chunk.fromIterable(1 to 1000).map(Right(_)) :+ Left(
            EntityDefect("Entity died with java.lang.Exception: boom")
          )
        )
      },
      test("Messages to an entity type that is not registered fail with TransportError") {
        for {
          _       <- Sharding.registerScoped
          counter <- Sharding.messenger(NeverRegistered)
          error   <- counter.send("c1")(GetCounter()).flip
        } yield assertTrue(error match {
          case TransportError(message) => message.contains("Entity type never-registered was not registered")
          case _                       => false
        })
      },
      test("Messages to an entity type whose registration closed are retried") {
        for {
          _       <- Sharding.registerScoped
          _       <- ZIO.scoped(Sharding.registerEntity(ClosedCounter)(behavior))
          counter <- Sharding.messenger(ClosedCounter, MessengerTimeout.Timeout(500.millis))
          error   <- counter.send("c1")(GetCounter()).flip
        } yield assertTrue(error == SendTimeout(ClosedCounter.name, "c1"))
      },
      test("A sendDiscard that can't be delivered times out even from an uninterruptible region") {
        for {
          _       <- Sharding.registerScoped
          _       <- ZIO.scoped(Sharding.registerEntity(ClosedCounter)(behavior))
          counter <- Sharding.messenger(ClosedCounter, MessengerTimeout.Timeout(500.millis))
          sent    <- counter.sendDiscard("c1")(GetCounter()).uninterruptible.fork
          exit    <- sent.await.timeout(5.seconds)
        } yield assertTrue(exit.contains(Exit.fail(SendTimeout(ClosedCounter.name, "c1"))))
      },
      test("An empty sendStream to an entity type that is not registered does nothing, like on a remote pod") {
        for {
          _       <- Sharding.registerScoped
          counter <- Sharding.messenger(NeverRegistered)
          _       <- counter.sendStream("c1")(ZStream.empty)
        } yield assertCompletes
      },
      test("Messages sent while the pod is stopping are retried, even after their entity type is unregistered") {
        for {
          _      <- Sharding.registerScoped
          state  <- WorkerState.make
          _      <- ZIO.scoped(Sharding.registerEntity(Worker)(worker(state)) *> Sharding.unregister)
          worker <- Sharding.messenger(Worker, MessengerTimeout.Timeout(500.millis))
          error  <- worker.send("w1")(Ping()).flip
        } yield assertTrue(error == SendTimeout(Worker.name, "w1"))
      },
      test("A getShardId that throws is a defect of the returned effect") {
        for {
          counter <- Sharding.messenger(UnhashableCounter)
          exit    <- counter.send("c1")(GetCounter()).exit
        } yield assertTrue(exit.causeOption.exists(_.isDie))
      },
      test("Response stream") {
        for {
          counter <- startCounter()
          latch   <- Promise.make[Nothing, Unit]
          fiber   <- counter.stream("c1")(StreamingChanges()).take(5).tap(_ => latch.succeed(())).runCollect.fork
          _       <- latch.await
          _       <- counter.sendDiscard("c1")(IncrementCounter())
          _       <- counter.sendDiscard("c1")(DecrementCounter())
          _       <- counter.sendDiscard("c1")(IncrementCounter())
          _       <- counter.sendDiscard("c1")(IncrementCounter())
          items   <- fiber.join
        } yield assertTrue(items == Chunk(0, 1, 0, 1, 2))
      },
      test("Response stream keeps the chunks of the entity's stream") {
        for {
          counter <- startCounter()
          chunks  <- counter.stream("c1")(Chunked()).chunks.runCollect
          restart <- counter.streamAutoRestart("c1", 0)(_ => Chunked())((c, _) => c).chunks.runCollect
        } yield assertTrue(chunks == Chunk(Chunk(1, 2, 3), Chunk(4, 5)), restart == chunks)
      } @@ TestAspect.tag(inProcess),
      test("A response stream that both fails and dies is a defect") {
        for {
          counter <- startCounter()
          error   <- counter.stream("c1")(FailingAndDyingStream()).runDrain.flip
        } yield assertTrue(error == EntityDefect("Entity died with java.lang.Exception: boom"))
      },
      test("Response stream interruption") {
        for {
          counter <- startCounter()
          latch   <- Promise.make[Nothing, Unit]
          fiber   <- counter.stream("c1")(StreamingChanges()).take(2).tap(_ => latch.succeed(())).runCollect.fork
          _       <- latch.await
          _       <- fiber.interrupt
          // the stream sets the counter to -1 when it ends
          _       <- counter.send("c1")(GetCounter()).repeatUntil(_ == -1)
        } yield assertCompletes
      },
      test("Response stream is cancelled when the entity stops") {
        for {
          counter <- startCounter()
          latch   <- Promise.make[Nothing, Unit]
          fiber   <- counter.stream("c1")(StreamingChanges()).tap(_ => latch.succeed(())).runDrain.flip.fork
          _       <- latch.await
          _       <- Sharding.terminateLocalEntity(Counter, "c1")
          error   <- fiber.join
          // before any reply too, once the inputs were read (even if still buffered): a new attempt couldn't get them again
          inputs  <- Queue.unbounded[Int]
          taken   <- Promise.make[Nothing, Unit]
          _       <- inputs.offer(1)
          summing <- counter
                       .duplex("c2")(Sum(), ZStream.fromQueue(inputs).tap(_ => taken.succeed(())).rechunk(2))
                       .runDrain
                       .flip
                       .fork
          _       <- taken.await
          // over gRPC, the inputs are taken before the entity starts
          _       <- Sharding.localEntities(Counter).repeatUntil(_.contains("c2"))
          _       <- Clock.sleep(100.millis) // the message reaches the mailbox right after the entity starts
          _       <- Sharding.terminateLocalEntity(Counter, "c2")
          summed  <- summing.join.timeout(10.seconds)
          logged  <- unhandledFailures
        } yield assertTrue(error == StreamCancelled, summed.contains(StreamCancelled), logged.isEmpty)
      },
      test("A stream message whose entity stops before its first element is sent again") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker)
          fiber  <- worker.stream("w1")(GatedStream()).runCollect.fork
          _      <- state.running.changes.takeUntil(_ == 1).runDrain
          _      <- Sharding.terminateLocalEntity(Worker, "w1")
          _      <- state.started.get.repeatUntil(_ == 2).timeout(5.seconds)
          _      <- state.gate.succeed(())
          items  <- fiber.join
        } yield assertTrue(items == Chunk(2))
      },
      autoRestartTest("Response stream auto restart")(
        _.streamAutoRestart("c1", 0)(from => StreamingChanges(from))((_, n) => n + 1)
      ),
      test("Entity manual termination") {
        for {
          counter <- startCounter()
          _       <- counter.sendDiscard("c3")(IncrementCounter())
          c0      <- counter.send("c3")(GetCounter())
          _       <- counter.send("c3")(TriggerTerminate())
          c1      <- counter.send("c3")(GetCounter()) // counter should be restarted
        } yield assertTrue(c0 == 1, c1 == 0)
      },
      test("Entity termination with extension") {
        for {
          state   <- startWorker(EntityOptions(maxIdleTime = Some(3.seconds)))
          worker  <- Sharding.messenger(Worker)
          _       <- worker.send("w1")(Ping())
          _       <- Clock.sleep(1.second)
          // the entity that receives this message is stopped 3s after it at the earliest
          stopped <- worker
                       .send("w1")(Instance())
                       .flatMap(instance => state.finalized.changes.takeUntil(_ >= instance).runDrain)
                       .timed
        } yield assertTrue(stopped._1 >= 3.seconds.minus(50.millis)) // idle time is measured in milliseconds
      },
      test("Entity is not stopped while a message is running") {
        for {
          state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
          worker <- Sharding.messenger(Worker)
          fiber  <- worker.send("w1")(Block()).fork
          _      <- Clock.sleep(2.seconds)
          before <- state.finalized.get
          _      <- worker.send("w1")(Unblock())
          _      <- fiber.join
          _      <- state.finalized.changes.takeUntil(_ == 1).runDrain
        } yield assertTrue(before == 0)
      },
      test("Messages still running are interrupted after entityTerminationTimeout, even Uninterruptible ones") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker, MessengerTimeout.NoTimeout)
          fiber  <- worker.send("w1")(Block()).flip.fork
          fiber2 <- worker.send("w3")(LongUninterruptible()).flip.fork
          _      <- state.running.get.repeatUntil(_ == 2)
          result <- (Sharding.terminateLocalEntity(Worker, "w1") *>
                      Sharding.terminateLocalEntity(Worker, "w3") *>
                      fiber.join.zip(fiber2.join)).timed
          after  <- state.running.get
          // replies are sent before the entities' finalizers run
          _      <- state.finalized.changes.takeUntil(_ == 2).runDrain
        } yield assertTrue(
          result._1 >= Config.default.entityTerminationTimeout,
          result._2 == (EntityInterrupted(Worker.name, "w1"), EntityInterrupted(Worker.name, "w3")),
          after == 0
        )
      },
      test("Entity terminated from a finalizer is still interrupted after the timeout") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker)
          _      <- worker.sendDiscard("w2")(Block())
          _      <- state.running.get.repeatUntil(_ == 1)
          _      <- ZIO.unit.ensuring(Sharding.terminateLocalEntity(Worker, "w2"))
          _      <- state.running.changes.takeUntil(_ == 0).runDrain
        } yield assertCompletes
      },
      test("A finalizer that never ends doesn't block the shard's rebalance, and the entity can start again") {
        for {
          _        <- Sharding.registerScoped
          state    <- WorkerState.make
          hung     <- Ref.make(true)
          _        <- Sharding.registerEntity(Worker)(entityId =>
                        worker(state)(entityId) <* ZIO.addFinalizer(hung.getAndSet(false).flatMap(ZIO.never.when(_)))
                      )
          worker   <- Sharding.messenger(Worker)
          sharding <- ZIO.service[Sharding]
          _        <- worker.send("w1")(Ping())
          shard     = sharding.getShardId(Worker, "w1")
          moved    <- sharding.unassign(Set(shard)).ensuring(sharding.assign(Set(shard))).timeout(10.seconds)
          ping     <- worker.send("w1")(Ping()).exit
        } yield assertTrue(moved.isDefined, ping.isSuccess)
      },
      test("The entity types of a shard stop in parallel when it is unassigned") {
        for {
          first    <- Promise.make[Nothing, Unit]
          second   <- Promise.make[Nothing, Unit]
          state    <- WorkerState.make
          // each finalizer waits for the other one, so stopping the types one at a time reaches the stop bound
          _        <- Sharding.registerEntity(Worker)(entityId =>
                        worker(state)(entityId) <* ZIO.addFinalizer(first.succeed(()) *> second.await)
                      )
          _        <- Sharding.registerEntity(Counter)(entityId =>
                        behavior(entityId) <* ZIO.addFinalizer(second.succeed(()) *> first.await)
                      )
          _        <- Sharding.registerScoped
          workers  <- Sharding.messenger(Worker)
          counters <- Sharding.messenger(Counter)
          sharding <- ZIO.service[Sharding]
          _        <- workers.send("w1")(Ping())
          _        <- counters.send("w1")(GetCounter())
          shard     = sharding.getShardId(Worker, "w1")
          moved    <- sharding.unassign(Set(shard)).timed.ensuring(sharding.assign(Set(shard)))
        } yield assertTrue(moved._1 < Config.default.entityTerminationTimeout)
      },
      test("A message stuck in uninterruptible code doesn't block the shard's rebalance") {
        val interceptor = new EntityInterceptor[WorkerMessage] {
          override def aroundMessage[E, A](
            recipientType: RecipientType[?],
            entityId: String,
            message: WorkerMessage[E, A]
          )(effect: IO[E, A]): IO[E, A] =
            effect.uninterruptible
        }
        for {
          state    <- startWorker(EntityOptions(interceptor = interceptor))
          worker   <- Sharding.messenger(Worker)
          sharding <- ZIO.service[Sharding]
          _        <- worker.sendDiscard("w1")(Gated())
          _        <- state.running.changes.takeUntil(_ == 1).runDrain
          shard     = sharding.getShardId(Worker, "w1")
          moved    <- sharding
                        .unassign(Set(shard))
                        .timeout(10.seconds)
                        .ensuring(state.gate.succeed(()) *> sharding.assign(Set(shard)))
        } yield assertTrue(moved.isDefined)
      },
      test("An interceptor dying after the end of the entity doesn't delay the shard's rebalance") {
        val interceptor = new EntityInterceptor[WorkerMessage] {
          override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(
            effect: IO[E, A]
          ): IO[E, A] =
            effect <* ZIO.dieMessage("boom")
        }
        for {
          _        <- startWorker(EntityOptions(interceptor = interceptor))
          worker   <- Sharding.messenger(Worker)
          sharding <- ZIO.service[Sharding]
          _        <- worker.send("w1")(Ping())
          shard     = sharding.getShardId(Worker, "w1")
          moved    <- sharding.unassign(Set(shard)).timed.ensuring(sharding.assign(Set(shard)))
          entities <- Sharding.localEntities(Worker)
          ping     <- worker.send("w1")(Ping()).exit
        } yield assertTrue(moved._1 < Config.default.entityTerminationTimeout, !entities.contains("w1"), ping.isSuccess)
      },
      test("An interceptor that never runs the entity doesn't block the shard's rebalance") {
        for {
          _        <- startWorker(EntityOptions(interceptor = neverRunsEntity))
          worker   <- Sharding.messenger(Worker, MessengerTimeout.NoTimeout)
          sharding <- ZIO.service[Sharding]
          sent     <- worker.send("w1")(Ping()).exit.fork
          _        <- Sharding.localEntities(Worker).repeatUntil(_.contains("w1"))
          _        <- Clock.sleep(100.millis) // the message reaches the mailbox right after the entity starts
          shard     = sharding.getShardId(Worker, "w1")
          moved    <- sharding.unassign(Set(shard)).timeout(10.seconds).ensuring(sharding.assign(Set(shard)))
          exit     <- sent.join.timeout(1.second)
        } yield assertTrue(moved.isDefined, exit.contains(Exit.fail(EntityInterrupted(Worker.name, "w1"))))
      },
      test("An interceptor that never runs the entity doesn't keep a stopped entity forever") {
        for {
          _      <- startWorker(EntityOptions(interceptor = neverRunsEntity))
          worker <- Sharding.messenger(Worker, MessengerTimeout.NoTimeout)
          sent   <- worker.send("w1")(Ping()).exit.fork
          _      <- Sharding.localEntities(Worker).repeatUntil(_.contains("w1"))
          _      <- Clock.sleep(100.millis) // the message reaches the mailbox right after the entity starts
          _      <- Sharding.terminateLocalEntity(Worker, "w1")
          exit   <- sent.join.timeout(10.seconds)
        } yield assertTrue(exit.contains(Exit.fail(EntityInterrupted(Worker.name, "w1"))))
      },
      test("An interceptor running the entity after the stop gave up on it doesn't start it") {
        for {
          gate       <- Promise.make[Nothing, Unit]
          ended      <- Promise.make[Nothing, Unit]
          interceptor = new EntityInterceptor[WorkerMessage] {
                          override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(
                            effect: IO[E, A]
                          ): IO[E, A] =
                            (gate.await *> effect).ensuring(ended.succeed(()))
                        }
          state      <- startWorker(EntityOptions(interceptor = interceptor))
          worker     <- Sharding.messenger(Worker)
          sharding   <- ZIO.service[Sharding]
          _          <- worker.sendDiscard("w1")(Ping())
          _          <- Sharding.localEntities(Worker).repeatUntil(_.contains("w1"))
          shard       = sharding.getShardId(Worker, "w1")
          _          <- sharding.unassign(Set(shard)).ensuring(sharding.assign(Set(shard)))
          _          <- gate.succeed(())
          _          <- ended.await
          started    <- state.started.get
        } yield assertTrue(started == 0)
      },
      test("A message sent from a finalizer is delivered even if the entity is stopping") {
        for {
          state   <- startWorker()
          worker  <- Sharding.messenger(Worker, MessengerTimeout.NoTimeout)
          _       <- worker.sendDiscard("w1")(Gated())
          _       <- state.running.changes.takeUntil(_ == 1).runDrain
          _       <- Sharding.terminateLocalEntity(Worker, "w1") // stopping until the gate opens
          started <- Promise.make[Nothing, Unit]
          result  <- Promise.make[Nothing, Exit[Any, Unit]]
          sender  <-
            (started.succeed(()) *> ZIO.never).ensuring(worker.send("w1")(Ping()).exit.flatMap(result.succeed)).fork
          _       <- started.await
          _       <- sender.interrupt.fork
          _       <- Clock.sleep(300.millis)                     // longer than the delay before trying again
          _       <- state.gate.succeed(())
          exit    <- result.await
        } yield assertTrue(exit.isSuccess)
      },
      test("A message sent with sendDiscard from a fiber being interrupted is delivered") {
        for {
          state   <- startWorker()
          worker  <- Sharding.messenger(Worker)
          started <- Promise.make[Nothing, Unit]
          sender  <- (started.succeed(()) *> ZIO.never).ensuring(worker.sendDiscard("w1")(CompleteNow()).ignore).fork
          _       <- started.await
          _       <- sender.interrupt
          _       <- state.completed.changes.takeUntil(_ == 1).runDrain.timeout(5.seconds)
          done    <- state.completed.get
        } yield assertTrue(done == 1)
      },
      test("Send timeout interrupts the message") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker, MessengerTimeout.Timeout(500.millis))
          error  <- worker.send("w1")(Block()).flip
          _      <- state.running.changes.takeUntil(_ == 0).runDrain
          ping   <- worker.send("w1")(Ping()).exit // the entity processes the next message
        } yield assertTrue(error == SendTimeout(Worker.name, "w1"), ping.isSuccess)
      },
      test("Uninterruptible messages complete after their sender stops waiting") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker)
          sender <- worker.send("w1")(Gated()).fork
          _      <- state.running.changes.takeUntil(_ == 1).runDrain
          _      <- sender.interrupt
          // gives the cancellation time to reach the entity, which must not interrupt the message
          _      <- Clock.sleep(200.millis)
          _      <- state.gate.succeed(())
          _      <- state.completed.changes.takeUntil(_ == 1).runDrain
        } yield assertCompletes
      },
      test("Uninterruptible messages are processed even if their sender stopped waiting before they started") {
        for {
          state   <- startWorker()
          worker  <- Sharding.messenger(Worker)
          // long enough for the message to reach the mailbox before the timeout
          timed   <- Sharding.messenger(Worker, MessengerTimeout.Timeout(1.second))
          blocked <- worker.send("w1")(Block()).fork
          _       <- state.running.get.repeatUntil(_ == 1)
          error   <- timed.send("w1")(SlowUninterruptible()).flip
          _       <- worker.send("w1")(Unblock())
          _       <- blocked.join
          _       <- worker.send("w1")(Ping()) // processed after SlowUninterruptible
          done    <- state.completed.get
        } yield assertTrue(error == SendTimeout(Worker.name, "w1"), done == 1)
      },
      test("Concurrent messages run alongside the others") {
        for {
          state   <- startWorker()
          worker  <- Sharding.messenger(Worker)
          blocked <- worker.send("w1")(Block()).fork
          _       <- state.running.get.repeatUntil(_ == 1)
          running <- worker.send("w1")(Running()) // would time out if it waited for Block
          _       <- worker.send("w1")(Unblock())
          _       <- blocked.join
        } yield assertTrue(running == 1)
      },
      test("Concurrency option") {
        for {
          state   <- startWorker(EntityOptions(concurrency = 2))
          worker  <- Sharding.messenger(Worker)
          _       <- worker.sendDiscard("w1")(Block())
          _       <- worker.sendDiscard("w1")(Block())
          _       <- worker.sendDiscard("w1")(Block())
          _       <- state.running.changes.takeUntil(_ == 2).runDrain
          running <- worker.send("w1")(Running())
          _       <- worker.send("w1")(Unblock())
        } yield assertTrue(running == 2)
      },
      test("Messages cancelled while waiting for a permit don't keep it") {
        for {
          state    <- startWorker(EntityOptions(concurrency = 2))
          worker   <- Sharding.messenger(Worker)
          impatient =
            Sharding.messenger(Worker, MessengerTimeout.Timeout(100.millis)).flatMap(_.send("w1")(Ping()).exit)
          _        <- worker.sendDiscard("w1")(Block())
          _        <- worker.sendDiscard("w1")(Block())
          _        <- state.running.get.repeatUntil(_ == 2)
          _        <- impatient.repeatN(3) // each one times out while waiting for a permit
          _        <- worker.send("w1")(Unblock())
          ping     <- worker.send("w1")(Ping()).exit
        } yield assertTrue(ping.isSuccess)
      },
      test("Messages cancelled while waiting for a permit never start") {
        for {
          state  <- startWorker(EntityOptions(concurrency = 2))
          worker <- Sharding.messenger(Worker)
          timed  <- Sharding.messenger(Worker, MessengerTimeout.Timeout(30.millis))
          _      <- ZIO.foreachDiscard(1 to 100) { i =>
                      val id = s"w$i"
                      for {
                        _ <- worker.sendDiscard(id)(Block())
                        _ <- worker.sendDiscard(id)(Block())
                        _ <- state.running.get.repeatUntil(_ == 2)
                        _ <- timed.send(id)(CompleteNow()).exit // times out while waiting for a permit
                        _ <- worker.send(id)(Unblock())
                        _ <- state.running.get.repeatUntil(_ == 0)
                      } yield ()
                    }
          _      <- worker.send("w100")(Ping())
          done   <- state.completed.get
        } yield assertTrue(done == 0)
      } @@ TestAspect.tag(inProcess),
      test("Cancelled stream messages don't prevent passivation") {
        for {
          state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
          worker <- Sharding.messenger(Worker)
          // the message completes after its sender gave up
          _      <- worker.stream("w1")(SlowStream()).runCollect.timeout(100.millis)
          _      <- state.finalized.changes.takeUntil(_ == 1).runDrain
        } yield assertCompletes
      },
      test("Stream messages whose sender is interrupted right as they reply don't prevent passivation") {
        for {
          state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
          worker <- Sharding.messenger(Worker)
          _      <- worker.send("w1")(Ping())
          _      <-
            ZIO.foreachDiscard(1 to 300)(i =>
              worker.stream("w1")(QuickStream()).runDrain.fork.flatMap(ZIO.yieldNow.repeatN(i % 100) *> _.interrupt)
            )
          _      <- state.finalized.changes.takeUntil(_ == 1).runDrain
        } yield assertCompletes
      },
      test("Messages replying with a stream can't be sent with sendDiscard") {
        typeCheck(
          """Sharding.messenger(CounterActor.Counter).map(_.sendDiscard("c1")(CounterActor.CounterMessage.StreamingChanges()))"""
        )
          .map(result => assertTrue(result.left.exists(_.contains("Use `stream` for Streamed replies"))))
      },
      test("Mailbox full") {
        for {
          state  <- startWorker(EntityOptions(mailboxCapacity = 2))
          worker <- Sharding.messenger(Worker)
          _      <- worker.sendDiscard("w1")(Block())
          _      <- worker.sendDiscard("w1")(Ping())
          error  <- worker.send("w1")(Ping()).flip
        } yield assertTrue(error == MailboxFull(Worker.name, "w1"))
      },
      test("Open reply streams don't count in the mailbox capacity and keep the entity running") {
        for {
          counter <- startCounter(EntityOptions(maxIdleTime = Some(500.millis), mailboxCapacity = 2))
          // open the streams one after the other, so that no more than one message is pending at a time
          streams <- ZIO.foreach(1 to 4)(_ =>
                       Promise
                         .make[Nothing, Unit]
                         .flatMap(started =>
                           counter.stream("c1")(StreamingChanges()).tap(_ => started.succeed(())).runDrain.fork <*
                             started.await
                         )
                     )
          _       <- counter.send("c1")(IncrementCounter())
          _       <- Clock.sleep(2.seconds)           // longer than maxIdleTime
          c1      <- counter.send("c1")(GetCounter()) // the entity was not restarted
          open    <- ZIO.foreach(streams)(_.poll)
          _       <- ZIO.foreachDiscard(streams)(_.interrupt)
        } yield assertTrue(c1 == 1, open.forall(_.isEmpty))
      } @@ TestAspect.timeout(30.seconds),
      test("Messages that don't load the entity are discarded") {
        for {
          state     <- startWorker(EntityOptions(loadEntity = [E, A] => _ match { case Ping() => false; case _ => true }))
          worker    <- Sharding.messenger(Worker)
          discarded <- worker.sendDiscard("w1")(Ping()).exit
          error     <- worker.send("w1")(Ping()).flip
          _         <- worker.send("w1")(Running())
          afterLoad <- worker.send("w1")(Ping()).exit
        } yield assertTrue(
          discarded.isSuccess,
          error == MessageDiscarded(Worker.name, "w1"),
          afterLoad.isSuccess
        )
      },
      test("A stream of messages stops at the first discarded message") {
        for {
          state     <- startWorker(EntityOptions(loadEntity = [E, A] => _ match { case Ping() => false; case _ => true }))
          worker    <- Sharding.messenger(Worker)
          error     <- worker.sendStream("w1")(ZStream(Ping(), SlowUninterruptible())).flip
          _         <- Clock.sleep(1.second)
          completed <- state.completed.get
        } yield assertTrue(error == MessageDiscarded(Worker.name, "w1"), completed == 0)
      },
      test("Entity that fails to start") {
        for {
          _       <- Sharding.registerEntity(Counter) {
                       case "bad"    => ZIO.fail(new Exception("cannot start"))
                       case "throws" => throw new RuntimeException("boom")
                       case entityId => behavior(entityId)
                     }
          _       <- Sharding.registerScoped
          counter <- Sharding.messenger(Counter)
          bad     <- counter.send("bad")(GetCounter()).flip
          thrown  <- counter.send("throws")(GetCounter()).flip
          again   <- counter.send("throws")(GetCounter()).flip // not stuck, the factory runs again
          good    <- counter.send("good")(GetCounter())
        } yield assertTrue(
          bad == EntityDefect("Entity died with java.lang.Exception: cannot start"),
          thrown == EntityDefect("Entity died with java.lang.RuntimeException: boom"),
          again == thrown,
          good == 0
        )
      },
      test("Entity and topic types must have unique names") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(IncrementCounter())
          entity  <- Sharding.registerEntity(Counter)(behavior).exit
          topic   <- Sharding.registerTopic(OtherCounter)(behavior).exit
          c1      <- counter.send("c1")(GetCounter()) // the entity is still running
        } yield assertTrue(
          List(entity, topic).forall(
            _.causeOption.flatMap(_.dieOption).exists(_.getMessage.contains("named counter"))
          ),
          c1 == 1
        )
      },
      test("Messages sent while the pod doesn't host the entity are retried") {
        for {
          counter <- startCounter()
          // the pod doesn't start entities anymore
          _       <- Sharding.unregister
          send    <- counter.send("c1")(IncrementCounter()).fork
          stream  <- counter.stream("c1")(Chunked()).runCollect.fork
          sent    <- counter.sendStream("c1")(ZStream(IncrementCounter())).fork
          _       <- Clock.sleep(500.millis)
          pending <- ZIO.foreach(List(send, stream, sent))(_.poll)
          _       <- Sharding.register
          _       <- send.join
          items   <- stream.join
          _       <- sent.join
          c1      <- counter.send("c1")(GetCounter())
        } yield assertTrue(pending.forall(_.isEmpty), items == Chunk(1, 2, 3, 4, 5), c1 == 2)
      },
      test("Cluster singleton") {
        for {
          p        <- Promise.make[Nothing, Unit]
          stopping <- Promise.make[Nothing, Unit]
          release  <- Promise.make[Nothing, Unit]
          _        <- Sharding.registerSingleton(
                        "singleton",
                        (p.succeed(()) *> ZIO.never).onInterrupt(stopping.succeed(()) *> release.await)
                      )
          _        <- Sharding.registerScoped
          res      <- p.await
          // assigning another shard doesn't wait for a singleton that is stopping
          sharding <- ZIO.service[Sharding]
          _        <- sharding.unassign(Set(2))
          moving   <- sharding.unassign(Set(1)).fork
          _        <- stopping.await
          assigned <- sharding.assign(Set(2)).timeout(1.second).ensuring(release.succeed(()))
          _        <- moving.join *> sharding.assign(Set(1))
        } yield assertTrue(() == res, assigned.isDefined)
      },
      test("Send stream") {
        for {
          counter <- startCounter()
          _       <- counter.sendStream("c1")(ZStream.fromIterable(1 to 1000).as(IncrementCounter()))
          c0      <- counter.send("c1")(GetCounter())
        } yield assertTrue(c0 == 1000)
      },
      test("Send stream fails if the stream fails, and dies if it dies") {
        for {
          counter <- startCounter()
          error   <-
            counter.sendStream("c1")(ZStream(IncrementCounter(), IncrementCounter()) ++ ZStream.fail("oops")).flip
          c0      <- counter.send("c1")(GetCounter())
          died    <-
            counter.sendStream("c1")(ZStream(IncrementCounter()) ++ ZStream.die(new RuntimeException("boom"))).exit
          cleanup <-
            counter
              .sendStream("c1")(
                (ZStream(IncrementCounter()) ++ ZStream.fail("oops")).ensuring(ZIO.die(new Exception("cleanup")))
              )
              .exit
          logged  <- unhandledFailures
        } yield assertTrue(
          error == "oops",
          c0 == 2,
          died.causeOption.flatMap(_.dieOption).exists(_.getMessage == "boom"),
          cleanup.causeOption.flatMap(_.dieOption).exists(_.getMessage == "cleanup"),
          logged.isEmpty
        )
      },
      test("Duplex: send a stream and receive a stream") {
        for {
          counter <- startCounter()
          items   <- counter
                       .duplex("c1")(AddAndStreamChanges(), ZStream.fromIterable(List(1, 2, 3)))
                       .takeUntil(_ == 6)
                       .runCollect
          // the stream sets the counter to -1 when it ends
          _       <- counter.send("c1")(GetCounter()).repeatUntil(_ == -1)
          empty   <- counter.duplex("c1")(Doubled(), ZStream.empty).runCollect
        } yield assertTrue(items.last == 6, empty.isEmpty)
      },
      test("Duplex: the returned stream fails if the input stream fails, even when the replies end with the inputs") {
        for {
          counter <- startCounter()
          results <- ZIO.foreach(1 to 20)(_ =>
                       counter.duplex("c1")(Doubled(), ZStream(1, 2, 3) ++ ZStream.fail("boom")).runCollect.either
                     )
          died    <- counter.duplex("c1")(Doubled(), ZStream(1) ++ ZStream.die(new Exception("boom"))).runCollect.exit
          logged  <- unhandledFailures
        } yield assertTrue(
          results.forall(_ == Left("boom")),
          died.causeOption.flatMap(_.dieOption).exists(_.getMessage == "boom"),
          logged.isEmpty
        )
      },
      test("Duplex: a reply that throws instead of returning a stream ends it with EntityDefect") {
        for {
          counter <- startCounter()
          error   <- counter.duplex("c1")(ThrowingDuplex(), ZStream(1)).runCollect.flip
        } yield assertTrue(error == EntityDefect("Entity died with java.lang.Exception: boom"))
      },
      autoRestartTest("Duplex auto restart")(
        _.duplexAutoRestart("c1", 0)(from => AddAndStreamChanges(from), _ => ZStream.empty)((_, n) => n + 1)
      ),
      test("A stream message processed while the entity is stopping is sent again") {
        for {
          state  <- startWorker()
          worker <- Sharding.messenger(Worker)
          _      <- worker.sendDiscard("w1")(SlowUninterruptible())
          fiber  <- worker.stream("w1")(QuickStream()).runCollect.fork
          _      <- Clock.sleep(100.millis)
          // the stream message is processed while the entity drains its mailbox
          _      <- Sharding.terminateLocalEntity(Worker, "w1")
          items  <- fiber.join
        } yield assertTrue(items == Chunk(1, 2, 3))
      },
      test("A stream of messages faster than the entity fails when its mailbox is full") {
        for {
          state  <- startWorker(EntityOptions(mailboxCapacity = 2))
          worker <- Sharding.messenger(Worker)
          error  <- worker.sendStream("w1")(ZStream(Gated(), Complete(), Complete(), Complete())).flip
          _      <- state.gate.succeed(())
          // accepted once the mailbox has room, and processed after the messages accepted before
          _      <- worker.send("w1")(Ping()).retryWhile(_.isInstanceOf[MailboxFull])
          done   <- state.completed.get
        } yield assertTrue(error == MailboxFull(Worker.name, "w1"), done == 2) // the messages accepted are processed
      },
      test("A stream of messages is sent again from the start when the entity moves") {
        for {
          state    <- startWorker()
          worker   <- Sharding.messenger(Worker)
          sharding <- ZIO.service[Sharding]
          first    <- Ref.make(true)
          // the first time, the pod stops hosting the entity after 2 messages, and hosts it again 500ms later
          moving    =
            ZStream.execute(
              ZIO.whenZIODiscard(first.getAndSet(false))(
                (Clock.sleep(500.millis) *> sharding.register).forkDaemon *> sharding.unregister
              )
            )
          _        <- worker.sendStream("w1")(ZStream(Complete(), Complete()) ++ moving ++ ZStream(Complete()))
          _        <- worker.send("w1")(Ping()) // processed after the messages of the stream
          done     <- state.completed.get
        } yield assertTrue(done == 5) // 2 messages processed before the move, then the 3 messages
      } @@ TestAspect.tag(inProcess),
      test("Entity is restarted after one of its finalizers died") {
        for {
          state     <- WorkerState.make
          finalized <- Promise.make[Nothing, Unit]
          _         <- Sharding.registerEntity(Worker)(entityId =>
                         worker(state)(entityId) <* ZIO.addFinalizer(finalized.succeed(()) *> ZIO.dieMessage("boom"))
                       )
          _         <- Sharding.registerScoped
          worker    <- Sharding.messenger(Worker)
          _         <- worker.send("w1")(Ping())
          _         <- Sharding.terminateLocalEntity(Worker, "w1")
          _         <- finalized.await
          ping      <- worker.send("w1")(Ping()).exit
        } yield assertTrue(ping.isSuccess)
      },
      test("Closing the scope of a registration stops its entities, and the type can be registered again") {
        for {
          _     <- Sharding.registerScoped
          state <- WorkerState.make
          _     <- ZIO.scoped(
                     Sharding.registerEntity(Worker)(worker(state)) *>
                       Sharding.messenger(Worker).flatMap(_.send("w1")(Ping()))
                   )
          fin   <- state.finalized.get
          _     <- Sharding.registerEntity(Worker)(worker(state))
          ping  <- Sharding.messenger(Worker).flatMap(_.send("w1")(Ping())).exit
        } yield assertTrue(fin == 1, ping.isSuccess)
      },
      test("An entity type stays registered until its entities have stopped") {
        for {
          _        <- Sharding.registerScoped
          state    <- WorkerState.make
          stopping <- Promise.make[Nothing, Unit]
          saved    <- Ref.make(false)
          scope    <- Scope.make
          _        <- scope.extend[Sharding](
                        Sharding.registerEntity(Worker)(entityId =>
                          worker(state)(entityId) <*
                            ZIO.addFinalizer(stopping.succeed(()) *> Clock.sleep(500.millis) *> saved.set(true))
                        )
                      )
          _        <- Sharding.messenger(Worker).flatMap(_.send("w1")(Ping()))
          closing  <- scope.close(Exit.unit).fork
          _        <- stopping.await
          during   <- Sharding.registerEntity(Worker)(worker(state)).exit
          _        <- closing.join
          stopped  <- saved.get
          _        <- Sharding.registerEntity(Worker)(worker(state))
          ping     <- Sharding.messenger(Worker).flatMap(_.send("w1")(Ping())).exit
        } yield assertTrue(
          during.causeOption.flatMap(_.dieOption).exists(_.getMessage.contains("named worker")),
          stopped,
          ping.isSuccess
        )
      },
      test("An interceptor's log annotations apply to the start, the messages and the finalizers of an entity") {
        val interceptor = new EntityInterceptor[WorkerMessage] {
          override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(
            effect: IO[E, A]
          ): IO[E, A] =
            ZIO.logAnnotate("entity", s"${recipientType.name}/$entityId")(effect)
        }
        for {
          state  <- WorkerState.make
          _      <- Sharding.registerEntity(Worker, EntityOptions(interceptor = interceptor))(entityId =>
                      ZIO.log("start") *> worker(state)(entityId) <* ZIO.addFinalizer(ZIO.log("finalizer"))
                    )
          _      <- Sharding.registerScoped
          worker <- Sharding.messenger(Worker)
          _      <- worker.send("w1")(Log())
          _      <- Sharding.terminateLocalEntity(Worker, "w1")
          // the worker's own finalizer runs after the one that logs
          _      <- state.finalized.changes.takeUntil(_ == 1).runDrain
          logs   <- ZTestLogger.logOutput
        } yield assertTrue(
          logs.collect { case log if log.annotations.get("entity").contains("worker/w1") => log.message() } ==
            Chunk("start", "message", "finalizer")
        )
      } @@ TestAspect.tag(inProcess),
      test("An entity runs with the context of its registration, not of the message that started it") {
        for {
          state  <- WorkerState.make
          _      <- ZIO.logAnnotate("registered", "worker")(
                      Sharding.registerEntity(Worker)(entityId => ZIO.log("start") *> worker(state)(entityId))
                    )
          _      <- Sharding.registerScoped
          worker <- Sharding.messenger(Worker)
          _      <- ZIO.logAnnotate("request", "first")(worker.send("w1")(Log()))
          _      <- ZIO.logAnnotate("request", "second")(worker.send("w1")(Log()))
          logs   <- ZTestLogger.logOutput
        } yield assertTrue(
          logs.collect {
            case log if log.annotations.contains("registered") || log.annotations.contains("request") =>
              log.message() -> log.annotations
          } == Chunk.fromIterable(List("start", "message", "message").map(_ -> Map("registered" -> "worker")))
        )
      },
      test("An interceptor wraps each message and gets its outcome") {
        for {
          outcomes   <- Ref.make(Chunk.empty[(Any, Exit[Any, Any])])
          interceptor = new EntityInterceptor[CounterMessage] {
                          override def aroundMessage[E, A](
                            recipientType: RecipientType[?],
                            entityId: String,
                            message: CounterMessage[E, A]
                          )(effect: IO[E, A]): IO[E, A] =
                            effect.onExit(exit => outcomes.update(_ :+ (message -> exit)))
                        }
          counter    <- startCounter(EntityOptions(interceptor = interceptor))
          _          <- counter.send("c1")(IncrementCounter())
          _          <- counter.send("c1")(Fail()).flip
          _          <- counter.send("c1")(GetCounter())
          items      <- counter.stream("c1")(Chunked()).runCollect
          recorded   <- outcomes.get
        } yield assertTrue(
          recorded.take(3) == Chunk(
            IncrementCounter() -> Exit.unit,
            Fail()             -> Exit.fail(CounterError("failed")),
            GetCounter()       -> Exit.succeed(1)
          ),
          // the creation of the stream, not the stream itself
          recorded.drop(3).map((message, exit) => message -> exit.exists(_.isInstanceOf[Streamed[?, ?]])) ==
            Chunk(Chunked() -> true),
          items == Chunk(1, 2, 3, 4, 5)
        )
      },
      test("Interceptors that die are treated like handlers that die") {
        val interceptor = new EntityInterceptor[CounterMessage] {
          override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(
            effect: IO[E, A]
          ): IO[E, A] =
            if (entityId == "bad") ZIO.dieMessage("bad entity") else effect

          override def aroundMessage[E, A](
            recipientType: RecipientType[?],
            entityId: String,
            message: CounterMessage[E, A]
          )(effect: IO[E, A]): IO[E, A] =
            if (message == DecrementCounter()) ZIO.dieMessage("bad message") else effect
        }
        for {
          counter <- startCounter(EntityOptions(interceptor = interceptor))
          bad     <- counter.send("bad")(GetCounter()).flip
          _       <- counter.send("c1")(IncrementCounter())
          died    <- counter.send("c1")(DecrementCounter()).flip
          c1      <- counter.send("c1")(GetCounter()) // counter should be restarted
        } yield assertTrue(
          bad == EntityDefect("Entity died with java.lang.RuntimeException: bad entity"),
          died == EntityDefect("Entity died with java.lang.RuntimeException: bad message"),
          c1 == 0
        )
      },
      test("Matching on the message in an interceptor refines its reply and error types") {
        for {
          counts     <- Ref.make(Chunk.empty[Int])
          errors     <- Ref.make(Chunk.empty[String])
          interceptor = new EntityInterceptor[CounterMessage] {
                          override def aroundMessage[E, A](
                            recipientType: RecipientType[?],
                            entityId: String,
                            message: CounterMessage[E, A]
                          )(effect: IO[E, A]): IO[E, A] =
                            message match {
                              case GetCounter() => effect.tap(count => counts.update(_ :+ count))
                              case Fail()       => effect.tapError(error => errors.update(_ :+ error.message))
                              case _            => effect
                            }
                        }
          counter    <- startCounter(EntityOptions(interceptor = interceptor))
          _          <- counter.send("c1")(IncrementCounter())
          _          <- counter.send("c1")(GetCounter())
          _          <- counter.send("c1")(Fail()).flip
          counts     <- counts.get
          errors     <- errors.get
        } yield assertTrue(counts == Chunk(1), errors == Chunk("failed"))
      },
      test("Running entities are listed from their start until their scope is closed") {
        for {
          state   <- WorkerState.make
          // the entities seen by the first entity to stop, from its finalizer
          seen    <- Promise.make[Nothing, Set[String]]
          _       <- Sharding.registerEntity(Worker)(entityId =>
                       worker(state)(entityId) <* ZIO.addFinalizer(Sharding.localEntities(Worker).flatMap(seen.succeed))
                     )
          counter <- startCounter()
          worker  <- Sharding.messenger(Worker)
          before  <- Sharding.localEntities(Worker)
          _       <- worker.send("w1")(Ping()) *> worker.send("w2")(Ping()) *> counter.send("c1")(GetCounter())
          started <- Sharding.localEntities(Worker)
          _       <- Sharding.terminateLocalEntity(Worker, "w1")
          closing <- seen.await
          // removed right after its scope is closed
          _       <- Sharding.localEntities(Worker).repeatUntil(_ == Set("w2"))
        } yield assertTrue(before.isEmpty, started == Set("w1", "w2"), closing == started)
      },
      test("Passivated entities are no longer listed") {
        for {
          state   <- startWorker(EntityOptions(maxIdleTime = Some(100.millis)))
          worker  <- Sharding.messenger(Worker)
          // a running message keeps the entity from being passivated
          blocked <- worker.send("w1")(Block()).fork
          _       <- state.running.get.repeatUntil(_ == 1)
          running <- Sharding.localEntities(Worker)
          _       <- worker.send("w1")(Unblock()) *> blocked.join
          _       <- Sharding.localEntities(Worker).repeatUntil(_.isEmpty)
        } yield assertTrue(running == Set("w1"))
      },
      test("Only the entities on the shards of this pod are listed") {
        for {
          _        <- startWorker()
          worker   <- Sharding.messenger(Worker)
          sharding <- ZIO.service[Sharding]
          _        <- worker.send("w1")(Ping()) *> worker.send("w2")(Ping())
          shard     = sharding.getShardId(Worker, "w1")
          // the shard of w1 moves to another pod, which returns once its entities have stopped
          moved    <- (sharding.unassign(Set(shard)) *> Sharding.localEntities(Worker))
                        .ensuring(sharding.assign(Set(shard)))
        } yield assertTrue(sharding.getShardId(Worker, "w2") != shard, moved == Set("w2"))
      },
      test("Messages replying with a value that a stream could inhabit can't be sent with send") {
        for {
          streamed <- typeCheck("summon[shard.Unary[shard.Streamed[Nothing, Int] | Int]]")
          duplex   <- typeCheck("summon[shard.Unary[shard.Duplex[String, Nothing, Int] | Int]]")
          any      <- typeCheck("summon[shard.Unary[Any]]")
          nothing  <- typeCheck("summon[shard.Unary[Nothing]]")
          int      <- typeCheck("summon[shard.Unary[Int | String]]")
        } yield assertTrue(
          streamed.isLeft,
          duplex.isLeft,
          any.left.exists(_.contains("could be a stream")),
          nothing.isLeft,
          int.isRight
        )
      }
    )

  private val neverRunsEntity: EntityInterceptor[WorkerMessage] = new EntityInterceptor[WorkerMessage] {
    override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(effect: IO[E, A]): IO[E, A] =
      ZIO.never *> effect
  }

  /**
   * An interceptor that logs around each entity and message.
   */
  private val logging: EntityInterceptor[AnyMessage] =
    new EntityInterceptor[AnyMessage] {
      override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(effect: IO[E, A]): IO[E, A] =
        ZIO.log("default entity") *> effect

      override def aroundMessage[E, A](recipientType: RecipientType[?], entityId: String, message: Any)(
        effect: IO[E, A]
      ): IO[E, A] =
        ZIO.log("default before") *> effect <* ZIO.log("default after")
    }

  // failures other than defects that ZIO logged as unhandled in the fibers of Shard, not in those of the tests
  private val unhandledFailures: UIO[Chunk[String]] =
    ZTestLogger.logOutput.map(_.collect {
      case log
          if log.message().contains("did not handle an error") && log.cause.dieOption.isEmpty &&
            !log.trace.toString.contains("Spec.scala") =>
        s"${log.trace}: ${log.cause.failures.mkString(", ")}"
    })

  private val defaultInterceptor: Spec[Sharding & Scope, Any] =
    test("The interceptor of Config wraps the one of EntityOptions") {
      val typed = new EntityInterceptor[CounterMessage] {
        override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(
          effect: IO[E, A]
        ): IO[E, A] =
          ZIO.log("type entity") *> effect

        override def aroundMessage[E, A](
          recipientType: RecipientType[?],
          entityId: String,
          message: CounterMessage[E, A]
        )(effect: IO[E, A]): IO[E, A] =
          ZIO.log("type before") *> effect <* ZIO.log("type after")
      }
      for {
        counter <- startCounter(EntityOptions(interceptor = typed))
        _       <- counter.send("c1")(IncrementCounter())
        logs    <- ZTestLogger.logOutput
      } yield assertTrue(
        logs.map(_.message()).filter(m => m.startsWith("default ") || m.startsWith("type ")) ==
          Chunk("default entity", "type entity", "default before", "type before", "type after", "default after")
      )
    }

  /**
   * Test that an auto-restarting stream resumes where it ended when the entity restarts.
   */
  private def autoRestartTest(name: String)(
    autoRestart: Messenger[CounterMessage] => ZStream[Any, Any, Int]
  ): Spec[Sharding & Scope, Any] =
    test(name) {
      for {
        counter <- startCounter()
        latch   <- Promise.make[Nothing, Unit]
        fiber   <- autoRestart(counter).tap(_ => latch.succeed(())).take(4).runCollect.fork
        _       <- latch.await
        _       <- counter.send("c1")(IncrementCounter())
        _       <- Sharding.terminateLocalEntity(Counter, "c1") // restarted from 0, the stream resumes from 2
        _       <- counter.send("c1")(Subscriptions()).repeatUntil(_ == 1)
        _       <- counter.send("c1")(IncrementCounter())
        _       <- counter.send("c1")(IncrementCounter())
        _       <- counter.send("c1")(IncrementCounter())
        items   <- fiber.join
      } yield assertTrue(items == Chunk(0, 1, 2, 3))
    }
}

object CounterActor {
  case class CounterError(message: String)

  enum CounterMessage[E, A] {
    case GetCounter()                       extends CounterMessage[Nothing, Int]
    case IncrementCounter()                 extends CounterMessage[Nothing, Unit]
    case DecrementCounter()                 extends CounterMessage[Nothing, Unit]
    case Fail()                             extends CounterMessage[CounterError, Unit]
    case FailAndDie()                       extends CounterMessage[CounterError, Unit]
    case Die()                              extends CounterMessage[Nothing, Unit]
    case StreamingChanges(from: Int = 0)    extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    // 1 to 1000, then dies
    case DyingStream()                      extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    case Chunked()                          extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    // fails with a cause that has both a failure and a defect
    case FailingAndDyingStream()            extends CounterMessage[CounterError, Streamed[CounterError, Int]]
    case AddAndStreamChanges(from: Int = 0) extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    // replies with each input doubled, and ends when the inputs end
    case Doubled()                          extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    // replies only once its inputs end
    case Sum()                              extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    // replies with a Duplex that throws when called
    case ThrowingDuplex()                   extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    case TriggerTerminate()                 extends CounterMessage[Nothing, Unit]
    // number of streams of changes that started on this entity
    case Subscriptions()                    extends CounterMessage[Nothing, Int]
  }

  object Counter extends EntityType[CounterMessage]("counter")

  object NeverRegistered extends EntityType[CounterMessage]("never-registered")

  object ClosedCounter extends EntityType[CounterMessage]("closed-counter")

  object UnhashableCounter extends EntityType[CounterMessage]("unhashable") {
    override def getShardId(entityId: String, numberOfShards: Int): ShardId = throw new Exception("boom")
  }

  // a different entity type with the same name
  object OtherCounter extends TopicType[CounterMessage]("counter")

  def startCounter(
    options: EntityOptions[CounterMessage] = EntityOptions()
  ): RIO[Sharding & Scope, Messenger[CounterMessage]] =
    Sharding.registerEntity(Counter, options)(behavior) *> Sharding.registerScoped *> Sharding.messenger(Counter)

  def behavior(entityId: String): URIO[Sharding & Scope, Counter.Handler] =
    for {
      sharding      <- ZIO.service[Sharding]
      state         <- SubscriptionRef.make(0)
      subscriptions <- Ref.make(0)
      _             <- ZIO.logInfo(s"Started entity $entityId")
    } yield new Counter.Handler {
      // counted once subscribed, when the current value is emitted
      private val changes: ZStream[Any, Nothing, Int] =
        state.changes.mapAccumZIO(true)((first, n) => subscriptions.update(_ + 1).when(first).as((false, n)))

      def handle[E, A](message: CounterMessage[E, A]): IO[E, A] =
        message match {
          case GetCounter()              => state.get
          case IncrementCounter()        => state.update(_ + 1)
          case DecrementCounter()        => state.update(_ - 1)
          case Fail()                    => ZIO.fail(CounterError("failed"))
          case FailAndDie()              => ZIO.fail(CounterError("failed")).ensuring(ZIO.die(new Exception("boom")))
          case Die()                     => ZIO.die(new Exception("boom"))
          case StreamingChanges(from)    => ZIO.succeed(Streamed(changes.filter(_ >= from).ensuring(state.set(-1))))
          case DyingStream()             =>
            ZIO.succeed(Streamed(ZStream.fromIterable(1 to 1000) ++ ZStream.die(new Exception("boom"))))
          case Chunked()                 => ZIO.succeed(Streamed(ZStream.fromChunks(Chunk(1, 2, 3), Chunk(4, 5))))
          case FailingAndDyingStream()   =>
            ZIO.succeed(
              Streamed(
                ZStream.fromChannel(
                  ZChannel.refailCause(Cause.fail(CounterError("failed")) && Cause.die(new Exception("boom")))
                )
              )
            )
          case AddAndStreamChanges(from) =>
            ZIO.succeed(
              Duplex[Int, Nothing, Int](input =>
                changes
                  .filter(_ >= from)
                  .drainFork(input.mapZIO(n => state.update(_ + n)))
                  .ensuring(state.set(-1))
              )
            )
          case Doubled()                 => ZIO.succeed(Duplex[Int, Nothing, Int](_.map(_ * 2)))
          case Sum()                     => ZIO.succeed(Duplex[Int, Nothing, Int](input => ZStream.fromZIO(input.runSum)))
          case ThrowingDuplex()          => ZIO.succeed(Duplex[Int, Nothing, Int](_ => throw new Exception("boom")))
          case TriggerTerminate()        => sharding.terminateLocalEntity(Counter, entityId)
          case Subscriptions()           => subscriptions.get
        }
    }

}

object WorkerActor {
  enum WorkerMessage[E, A] {
    // waits until Unblock
    case Block()               extends WorkerMessage[Nothing, Unit]
    case Unblock()             extends WorkerMessage[Nothing, Unit] with Concurrent
    case Ping()                extends WorkerMessage[Nothing, Unit]
    // number of Block messages running
    case Running()             extends WorkerMessage[Nothing, Int] with Concurrent
    case SlowUninterruptible() extends WorkerMessage[Nothing, Unit] with Uninterruptible
    // counts as running for 10s
    case LongUninterruptible() extends WorkerMessage[Nothing, Unit] with Uninterruptible
    case SlowStream()          extends WorkerMessage[Nothing, Streamed[Nothing, Int]] with Uninterruptible
    case QuickStream()         extends WorkerMessage[Nothing, Streamed[Nothing, Int]]
    // counts as running until the test opens the gate, then emits the number of the entity
    case GatedStream()         extends WorkerMessage[Nothing, Streamed[Nothing, Int]]
    // takes 10ms, then counts as completed
    case Complete()            extends WorkerMessage[Nothing, Unit]
    // counts as completed right away
    case CompleteNow()         extends WorkerMessage[Nothing, Unit]
    // counts as running until the test opens the gate, then counts as completed
    case Gated()               extends WorkerMessage[Nothing, Unit] with Uninterruptible
    // number of the entity, counting from 1 each time it starts
    case Instance()            extends WorkerMessage[Nothing, Int]
    // logs "message"
    case Log()                 extends WorkerMessage[Nothing, Unit]
  }

  object Worker extends EntityType[WorkerMessage]("worker")

  def startWorker(options: EntityOptions[WorkerMessage] = EntityOptions()): RIO[Sharding & Scope, WorkerState] =
    WorkerState.make.tap(state => Sharding.registerEntity(Worker, options)(worker(state)) *> Sharding.registerScoped)

  case class WorkerState(
    running: SubscriptionRef[Int],
    finalized: SubscriptionRef[Int],
    completed: SubscriptionRef[Int],
    started: Ref[Int],
    gate: Promise[Nothing, Unit]
  )

  object WorkerState {
    val make: UIO[WorkerState] =
      for {
        running   <- SubscriptionRef.make(0)
        finalized <- SubscriptionRef.make(0)
        completed <- SubscriptionRef.make(0)
        started   <- Ref.make(0)
        gate      <- Promise.make[Nothing, Unit]
      } yield WorkerState(running, finalized, completed, started, gate)
  }

  def worker(state: WorkerState)(entityId: String): URIO[Scope, Worker.Handler] =
    for {
      unblock  <- Promise.make[Nothing, Unit]
      instance <- state.started.updateAndGet(_ + 1)
      _        <- ZIO.addFinalizer(state.finalized.update(_ + 1))
    } yield new Worker.Handler {
      private def running(effect: UIO[Unit]): UIO[Unit] =
        ZIO.acquireReleaseWith(state.running.update(_ + 1))(_ => state.running.update(_ - 1))(_ => effect)

      def handle[E, A](message: WorkerMessage[E, A]): IO[E, A] =
        message match {
          case Block()               => running(unblock.await)
          case LongUninterruptible() => running(Clock.sleep(10.seconds))
          case Unblock()             => unblock.succeed(()).unit
          case Ping()                => ZIO.unit
          case Running()             => state.running.get
          case SlowUninterruptible() => Clock.sleep(500.millis) *> state.completed.update(_ + 1)
          case SlowStream()          => Clock.sleep(500.millis).as(Streamed(ZStream(1, 2, 3)))
          case QuickStream()         => ZIO.succeed(Streamed(ZStream(1, 2, 3)))
          case GatedStream()         => ZIO.succeed(Streamed(ZStream.fromZIO(running(state.gate.await).as(instance))))
          case Complete()            => Clock.sleep(10.millis) *> state.completed.update(_ + 1)
          case CompleteNow()         => state.completed.update(_ + 1)
          case Gated()               => running(state.gate.await) *> state.completed.update(_ + 1)
          case Instance()            => ZIO.succeed(instance)
          case Log()                 => ZIO.log("message")
        }
    }
}
