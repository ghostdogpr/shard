package shard

import shard.Messenger.MessengerTimeout
import shard.CounterActor.CounterMessage._
import shard.CounterActor._
import shard.WorkerActor.WorkerMessage._
import shard.WorkerActor._
import shard.errors._
import shard.interfaces.Storage
import shard.internal.LocalShardManagerClient
import shard.javaSerialization.given
import zio.stream.{ SubscriptionRef, ZStream }
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
        ZLayer.succeed(Config.default.copy(entityInterceptor = logging("default")))
      )
    ) @@ sequential @@ withLiveClock @@ TestAspect.timeout(60.seconds)

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
        } yield assertTrue(error == CounterError("failed"), c1 == 1)
      },
      test("Defect stops the entity") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(IncrementCounter())
          error   <- counter.send("c1")(Die()).flip
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
          counter <- Sharding.messenger(Counter)
          error   <- counter.send("c1")(GetCounter()).flip
        } yield assertTrue(error match {
          case TransportError(message) => message.contains("Entity type counter was not registered")
          case _                       => false
        })
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
        } yield assertTrue(error == StreamCancelled)
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
          state     <- startWorker(EntityOptions(loadEntity = { case Ping() => false; case _ => true }))
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
          state     <- startWorker(EntityOptions(loadEntity = { case Ping() => false; case _ => true }))
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
          p   <- Promise.make[Nothing, Unit]
          _   <- Sharding.registerSingleton("singleton", p.succeed(()) *> ZIO.never)
          _   <- Sharding.registerScoped
          res <- p.await
        } yield assertTrue(() == res)
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
        } yield assertTrue(
          error == "oops",
          c0 == 2,
          died.causeOption.flatMap(_.dieOption).exists(_.getMessage == "boom")
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
        } yield assertTrue(
          results.forall(_ == Left("boom")),
          died.causeOption.flatMap(_.dieOption).exists(_.getMessage == "boom")
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
        val interceptor = new EntityInterceptor {
          override def aroundEntity[E, A](recipientType: String, entityId: String)(effect: IO[E, A]): IO[E, A] =
            ZIO.logAnnotate("entity", s"$recipientType/$entityId")(effect)
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
      test("Logs about an entity are annotated with its type and ID") {
        for {
          counter <- startCounter()
          _       <- counter.send("c1")(Die()).flip
          logs    <- ZTestLogger.logOutput
          died     = logs.filter(_.message().contains("died while processing a message"))
        } yield assertTrue(
          died.map(_.annotations) == Chunk(Map("entityType" -> Counter.name, "entityId" -> "c1"))
        )
      } @@ TestAspect.tag(inProcess),
      test("An interceptor wraps each message and gets its outcome") {
        for {
          outcomes   <- Ref.make(Chunk.empty[(Any, Exit[Any, Any])])
          interceptor = new EntityInterceptor {
                          override def aroundMessage[E, A](recipientType: String, entityId: String, message: Any)(
                            effect: IO[E, A]
                          ): IO[E, A] =
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
        val interceptor = new EntityInterceptor {
          override def aroundEntity[E, A](recipientType: String, entityId: String)(effect: IO[E, A]): IO[E, A] =
            if (entityId == "bad") ZIO.dieMessage("bad entity") else effect

          override def aroundMessage[E, A](recipientType: String, entityId: String, message: Any)(
            effect: IO[E, A]
          ): IO[E, A] =
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

  /**
   * An interceptor that logs before and after each message.
   */
  private def logging(name: String): EntityInterceptor =
    new EntityInterceptor {
      override def aroundMessage[E, A](recipientType: String, entityId: String, message: Any)(
        effect: IO[E, A]
      ): IO[E, A] =
        ZIO.log(s"$name before") *> effect <* ZIO.log(s"$name after")
    }

  private val defaultInterceptor: Spec[Sharding & Scope, Any] =
    test("The interceptor of Config wraps the one of EntityOptions") {
      for {
        counter <- startCounter(EntityOptions(interceptor = logging("type")))
        _       <- counter.send("c1")(IncrementCounter())
        logs    <- ZTestLogger.logOutput
      } yield assertTrue(
        logs.map(_.message()).filter(m => m.endsWith(" before") || m.endsWith(" after")) ==
          Chunk("default before", "type before", "type after", "default after")
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

  enum CounterMessage[+E, +A] {
    case GetCounter()                       extends CounterMessage[Nothing, Int]
    case IncrementCounter()                 extends CounterMessage[Nothing, Unit]
    case DecrementCounter()                 extends CounterMessage[Nothing, Unit]
    case Fail()                             extends CounterMessage[CounterError, Unit]
    case Die()                              extends CounterMessage[Nothing, Unit]
    case StreamingChanges(from: Int = 0)    extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    // 1 to 1000, then dies
    case DyingStream()                      extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    case Chunked()                          extends CounterMessage[Nothing, Streamed[Nothing, Int]]
    case AddAndStreamChanges(from: Int = 0) extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    // replies with each input doubled, and ends when the inputs end
    case Doubled()                          extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    // replies with a Duplex that throws when called
    case ThrowingDuplex()                   extends CounterMessage[Nothing, Duplex[Int, Nothing, Int]]
    case TriggerTerminate()                 extends CounterMessage[Nothing, Unit]
    // number of streams of changes that started on this entity
    case Subscriptions()                    extends CounterMessage[Nothing, Int]
  }

  object Counter extends EntityType[CounterMessage]("counter")

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
          case Die()                     => ZIO.die(new Exception("boom"))
          case StreamingChanges(from)    => ZIO.succeed(Streamed(changes.filter(_ >= from).ensuring(state.set(-1))))
          case DyingStream()             =>
            ZIO.succeed(Streamed(ZStream.fromIterable(1 to 1000) ++ ZStream.die(new Exception("boom"))))
          case Chunked()                 => ZIO.succeed(Streamed(ZStream.fromChunks(Chunk(1, 2, 3), Chunk(4, 5))))
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
          case ThrowingDuplex()          => ZIO.succeed(Duplex[Int, Nothing, Int](_ => throw new Exception("boom")))
          case TriggerTerminate()        => sharding.terminateLocalEntity(Counter, entityId)
          case Subscriptions()           => subscriptions.get
        }
    }

}

object WorkerActor {
  enum WorkerMessage[+E, +A] {
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
          case Complete()            => Clock.sleep(10.millis) *> state.completed.update(_ + 1)
          case CompleteNow()         => state.completed.update(_ + 1)
          case Gated()               => running(state.gate.await) *> state.completed.update(_ + 1)
          case Instance()            => ZIO.succeed(instance)
          case Log()                 => ZIO.log("message")
        }
    }
}
