package shard

import shard.Messenger.MessengerTimeout
import shard.CounterActor.CounterMessage._
import shard.CounterActor._
import shard.WorkerActor.WorkerMessage._
import shard.WorkerActor._
import shard.errors._
import shard.interfaces.Storage
import shard.javaSerialization.given
import zio.stream.{ SubscriptionRef, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object ShardingSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ShardingSpec")(
      suite("local-pod shortcut")(tests*).provideShared(
        LocalSharding.live,
        ShardManagerClient.local,
        Storage.memory,
        ZLayer.succeed(Config.default)
      ),
      suite("simulate remote pods (forces serialization on every hop)")(tests*).provideShared(
        LocalSharding.live,
        ShardManagerClient.local,
        Storage.memory,
        ZLayer.succeed(Config.default.copy(simulateRemotePods = true))
      )
    ) @@ sequential @@ withLiveClock

  // tests relying on the in-process transport's chunking and delivery order
  val inProcess: String = "in-process"

  val tests: List[Spec[Sharding, Any]] =
    List(
      test("Send message to entities") {
        ZIO.scoped {
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
        }
      },
      test("Typed failure") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            _       <- counter.send("c1")(IncrementCounter())
            error   <- counter.send("c1")(Fail()).flip
            c1      <- counter.send("c1")(GetCounter()) // the entity is still running
          } yield assertTrue(error == CounterError("failed"), c1 == 1)
        }
      },
      test("Defect stops the entity") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            _       <- counter.send("c1")(IncrementCounter())
            error   <- counter.send("c1")(Die()).flip
            c1      <- counter.send("c1")(GetCounter()) // counter should be restarted
          } yield assertTrue(error == EntityDefect("Entity died with java.lang.Exception: boom"), c1 == 0)
        }
      },
      test("A defect in a response stream ends it with EntityDefect, after its elements") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            items   <- counter.stream("c1")(DyingStream()).either.runCollect
          } yield assertTrue(
            items == Chunk.fromIterable(1 to 1000).map(Right(_)) :+ Left(
              EntityDefect("Entity died with java.lang.Exception: boom")
            )
          )
        }
      },
      test("Messages to an entity type that is not registered fail with TransportError") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            error   <- counter.send("c1")(GetCounter()).flip
          } yield assertTrue(error match {
            case TransportError(message) => message.contains("Entity type counter was not registered")
            case _                       => false
          })
        }
      },
      test("Response stream") {
        ZIO.scoped {
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
        }
      },
      test("Response stream keeps the chunks of the entity's stream") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            chunks  <- counter.stream("c1")(Chunked()).chunks.runCollect
            restart <- counter.streamAutoRestart("c1", 0)(_ => Chunked())((c, _) => c).chunks.runCollect
          } yield assertTrue(chunks == Chunk(Chunk(1, 2, 3), Chunk(4, 5)), restart == chunks)
        }
      } @@ TestAspect.tag(inProcess),
      test("Response stream interruption") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- counter.stream("c1")(StreamingChanges()).take(2).tap(_ => latch.succeed(())).runCollect.fork
            _       <- latch.await
            _       <- fiber.interrupt
            _       <- Clock.sleep(1 second)
            res     <- counter.send("c1")(GetCounter())
          } yield assertTrue(res == -1)
        }
      },
      test("Response stream is cancelled when the entity stops") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- counter.stream("c1")(StreamingChanges()).tap(_ => latch.succeed(())).runDrain.flip.fork
            _       <- latch.await
            _       <- Sharding.terminateLocalEntity(Counter, "c1")
            error   <- fiber.join
          } yield assertTrue(error == StreamCancelled)
        }
      },
      autoRestartTest("Response stream auto restart")(
        _.streamAutoRestart("c1", 0)(from => StreamingChanges(from))((_, n) => n + 1)
      ),
      test("Entity manual termination") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            _       <- counter.sendDiscard("c3")(IncrementCounter())
            c0      <- counter.send("c3")(GetCounter())
            _       <- counter.send("c3")(TriggerTerminate())
            c1      <- counter.send("c3")(GetCounter()) // counter should be restarted
          } yield assertTrue(c0 == 1, c1 == 0)
        }
      },
      test("Entity termination with extension") {
        ZIO.scoped {
          for {
            counter <- startCounter(EntityOptions(maxIdleTime = Some(3.seconds)))
            _       <- counter.sendDiscard("c3")(IncrementCounter())
            c0      <- counter.send("c3")(GetCounter())
            _       <- Clock.sleep(2 seconds)
            _       <- counter.sendDiscard("c3")(IncrementCounter())
            c1      <- counter.send("c3")(GetCounter())
            _       <- Clock.sleep(4 seconds)
            c2      <- counter.send("c3")(GetCounter()) // counter should be restarted
          } yield assertTrue(c0 == 1, c1 == 2, c2 == 0)
        }
      },
      test("Entity is not stopped while a message is running") {
        ZIO.scoped {
          for {
            state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
            worker <- Sharding.messenger(Worker)
            fiber  <- worker.send("w1")(Block()).fork
            _      <- Clock.sleep(2.seconds)
            before <- state.finalized.get
            _      <- worker.send("w1")(Unblock())
            _      <- fiber.join
            _      <- Clock.sleep(2.seconds)
            after  <- state.finalized.get
          } yield assertTrue(before == 0, after == 1)
        }
      },
      test("Messages still running are interrupted after entityTerminationTimeout, even Uninterruptible ones") {
        ZIO.scoped {
          for {
            state  <- startWorker()
            worker <- Sharding.messenger(Worker, MessengerTimeout.NoTimeout)
            fiber  <- worker.send("w1")(Block()).flip.fork
            fiber2 <- worker.send("w3")(LongUninterruptible()).either.fork
            _      <- state.running.get.repeatUntil(_ == 2)
            _      <- Sharding.terminateLocalEntity(Worker, "w1")
            _      <- Sharding.terminateLocalEntity(Worker, "w3")
            // 3s of termination timeout (Config.default)
            _      <- Clock.sleep(2.seconds)
            during <- state.running.get
            error  <- fiber.join
            error2 <- fiber2.join.timeout(5.seconds)
            after  <- state.running.get
            fin    <- state.finalized.get
          } yield assertTrue(
            during == 2,
            error == EntityInterrupted(Worker.name, "w1"),
            error2.contains(Left(EntityInterrupted(Worker.name, "w3"))),
            after == 0,
            fin == 2
          )
        }
      },
      test("Entity terminated from a finalizer is still interrupted after the timeout") {
        ZIO.scoped {
          for {
            state  <- startWorker()
            worker <- Sharding.messenger(Worker)
            _      <- worker.sendDiscard("w2")(Block())
            _      <- state.running.get.repeatUntil(_ == 1)
            _      <- ZIO.unit.ensuring(Sharding.terminateLocalEntity(Worker, "w2"))
            // 3s of termination timeout (Config.default)
            _      <- Clock.sleep(5.seconds)
            after  <- state.running.get
          } yield assertTrue(after == 0)
        }
      },
      test("Send timeout interrupts the message") {
        ZIO.scoped {
          for {
            state  <- startWorker()
            worker <- Sharding.messenger(Worker, MessengerTimeout.Timeout(500.millis))
            error  <- worker.send("w1")(Block()).flip
            _      <- Clock.sleep(200.millis)
            after  <- state.running.get
            ping   <- worker.send("w1")(Ping()).exit // the entity processes the next message
          } yield assertTrue(error == SendTimeout(Worker.name, "w1"), after == 0, ping.isSuccess)
        }
      },
      test("Uninterruptible messages complete after a timeout") {
        ZIO.scoped {
          for {
            state  <- startWorker()
            worker <- Sharding.messenger(Worker, MessengerTimeout.Timeout(200.millis))
            error  <- worker.send("w1")(SlowUninterruptible()).flip
            _      <- Clock.sleep(1.second)
            done   <- state.completed.get
          } yield assertTrue(error == SendTimeout(Worker.name, "w1"), done == 1)
        }
      },
      test("Uninterruptible messages are processed even if their sender stopped waiting before they started") {
        ZIO.scoped {
          for {
            state   <- startWorker()
            worker  <- Sharding.messenger(Worker)
            timed   <- Sharding.messenger(Worker, MessengerTimeout.Timeout(200.millis))
            blocked <- worker.send("w1")(Block()).fork
            _       <- state.running.get.repeatUntil(_ == 1)
            error   <- timed.send("w1")(SlowUninterruptible()).flip
            _       <- worker.send("w1")(Unblock())
            _       <- blocked.join
            _       <- worker.send("w1")(Ping()) // processed after SlowUninterruptible
            done    <- state.completed.get
          } yield assertTrue(error == SendTimeout(Worker.name, "w1"), done == 1)
        }
      },
      test("Concurrent messages run alongside the others") {
        ZIO.scoped {
          for {
            state   <- startWorker()
            worker  <- Sharding.messenger(Worker)
            blocked <- worker.send("w1")(Block()).fork
            _       <- state.running.get.repeatUntil(_ == 1)
            running <- worker.send("w1")(Running()) // would time out if it waited for Block
            _       <- worker.send("w1")(Unblock())
            _       <- blocked.join
          } yield assertTrue(running == 1)
        }
      },
      test("Concurrency option") {
        ZIO.scoped {
          for {
            state   <- startWorker(EntityOptions(concurrency = 2))
            worker  <- Sharding.messenger(Worker)
            _       <- worker.sendDiscard("w1")(Block())
            _       <- worker.sendDiscard("w1")(Block())
            _       <- worker.sendDiscard("w1")(Block())
            _       <- Clock.sleep(500.millis)
            running <- worker.send("w1")(Running())
            _       <- worker.send("w1")(Unblock())
          } yield assertTrue(running == 2)
        }
      },
      test("Messages cancelled while waiting for a permit don't keep it") {
        ZIO.scoped {
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
        }
      },
      test("Cancelled stream messages don't prevent passivation") {
        ZIO.scoped {
          for {
            state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
            worker <- Sharding.messenger(Worker)
            // the message completes after its sender gave up
            _      <- worker.stream("w1")(SlowStream()).runCollect.timeout(100.millis)
            _      <- Clock.sleep(2.seconds)
            fin    <- state.finalized.get
          } yield assertTrue(fin == 1)
        }
      },
      test("Stream messages whose sender is interrupted right as they reply don't prevent passivation") {
        ZIO.scoped {
          for {
            state  <- startWorker(EntityOptions(maxIdleTime = Some(500.millis)))
            worker <- Sharding.messenger(Worker)
            _      <- worker.send("w1")(Ping())
            _      <-
              ZIO.foreachDiscard(1 to 300)(i =>
                worker.stream("w1")(QuickStream()).runDrain.fork.flatMap(ZIO.yieldNow.repeatN(i % 100) *> _.interrupt)
              )
            _      <- Clock.sleep(2.seconds)
            fin    <- state.finalized.get
          } yield assertTrue(fin == 1)
        }
      },
      test("Messages replying with a stream can't be sent with sendDiscard") {
        typeCheck(
          """Sharding.messenger(CounterActor.Counter).map(_.sendDiscard("c1")(CounterActor.CounterMessage.StreamingChanges()))"""
        )
          .map(result => assertTrue(result.left.exists(_.contains("Use `stream` for Streamed replies"))))
      },
      test("Mailbox full") {
        ZIO.scoped {
          for {
            state  <- startWorker(EntityOptions(mailboxCapacity = 2))
            worker <- Sharding.messenger(Worker)
            _      <- worker.sendDiscard("w1")(Block())
            _      <- worker.sendDiscard("w1")(Ping())
            error  <- worker.send("w1")(Ping()).flip
          } yield assertTrue(error == MailboxFull(Worker.name, "w1"))
        }
      },
      test("Open reply streams don't count in the mailbox capacity and keep the entity running") {
        ZIO.scoped {
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
        }
      } @@ TestAspect.timeout(30.seconds),
      test("Messages that don't load the entity are discarded") {
        ZIO.scoped {
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
        }
      },
      test("A stream of messages stops at the first discarded message") {
        ZIO.scoped {
          for {
            state     <- startWorker(EntityOptions(loadEntity = { case Ping() => false; case _ => true }))
            worker    <- Sharding.messenger(Worker)
            error     <- worker.sendStream("w1")(ZStream(Ping(), SlowUninterruptible())).flip
            _         <- Clock.sleep(1.second)
            completed <- state.completed.get
          } yield assertTrue(error == MessageDiscarded(Worker.name, "w1"), completed == 0)
        }
      },
      test("Entity that fails to start") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter)(entityId =>
                         if (entityId == "bad") ZIO.fail(new Exception("cannot start")) else behavior(entityId)
                       )
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            bad     <- counter.send("bad")(GetCounter()).flip
            good    <- counter.send("good")(GetCounter())
          } yield assertTrue(bad == EntityDefect("Entity died with java.lang.Exception: cannot start"), good == 0)
        }
      },
      test("Entity and topic types must have unique names") {
        ZIO.scoped {
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
        }
      },
      test("Messages sent while the pod doesn't host the entity are retried") {
        ZIO.scoped {
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
        }
      },
      test("Cluster singleton") {
        ZIO.scoped {
          for {
            p   <- Promise.make[Nothing, Unit]
            _   <- Sharding.registerSingleton("singleton", p.succeed(()) *> ZIO.never)
            _   <- Sharding.registerScoped
            res <- p.await
          } yield assertTrue(() == res)
        }
      },
      test("Send stream") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            _       <- counter.sendStream("c1")(ZStream.fromIterable(1 to 1000).as(IncrementCounter()))
            c0      <- counter.send("c1")(GetCounter())
          } yield assertTrue(c0 == 1000)
        }
      },
      test("Send stream fails if the stream fails, and dies if it dies") {
        ZIO.scoped {
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
        }
      },
      test("Duplex: send a stream and receive a stream") {
        ZIO.scoped {
          for {
            counter <- startCounter()
            items   <- counter
                         .duplex("c1")(AddAndStreamChanges(), ZStream.fromIterable(List(1, 2, 3)))
                         .takeUntil(_ == 6)
                         .runCollect
            _       <- Clock.sleep(1 second)
            res     <- counter.send("c1")(GetCounter())
            empty   <- counter.duplex("c1")(Doubled(), ZStream.empty).runCollect
          } yield assertTrue(items.last == 6, res == -1, empty.isEmpty)
        }
      },
      test("Duplex: the returned stream fails if the input stream fails, even when the replies end with the inputs") {
        ZIO.scoped {
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
        }
      },
      autoRestartTest("Duplex auto restart")(
        _.duplexAutoRestart("c1", 0)(from => AddAndStreamChanges(from), _ => ZStream.empty)((_, n) => n + 1)
      ),
      test("A stream message processed while the entity is stopping is sent again") {
        ZIO.scoped {
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
        }
      },
      test("A stream of messages faster than the entity fails when its mailbox is full") {
        ZIO.scoped {
          for {
            state  <- startWorker(EntityOptions(mailboxCapacity = 2))
            worker <- Sharding.messenger(Worker)
            error  <- worker.sendStream("w1")(ZStream(SlowUninterruptible(), Complete(), Complete(), Complete())).flip
            _      <- Clock.sleep(1.second)
            _      <- worker.send("w1")(Ping())
            done   <- state.completed.get
          } yield assertTrue(error == MailboxFull(Worker.name, "w1"), done == 2) // the messages accepted are processed
        }
      },
      test("A stream of messages is sent again from the start when the entity moves") {
        ZIO.scoped {
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
        }
      } @@ TestAspect.tag(inProcess),
      test("Entity is restarted after one of its finalizers died") {
        ZIO.scoped {
          for {
            state     <- WorkerState.make
            finalized <- Promise.make[Nothing, Unit]
            _         <- Sharding.registerEntity(Worker)(entityId =>
                           worker(state)(entityId) <* ZIO.addFinalizer(finalized.succeed(()) *> ZIO.dieMessage("boom"))
                         )
            _         <- Sharding.registerScoped
            worker    <- Sharding.messenger(Worker, MessengerTimeout.Timeout(2.seconds))
            _         <- worker.send("w1")(Ping())
            _         <- Sharding.terminateLocalEntity(Worker, "w1")
            _         <- finalized.await
            ping      <- worker.send("w1")(Ping()).exit
          } yield assertTrue(ping.isSuccess)
        }
      },
      test("Closing the scope of a registration stops its entities, and the type can be registered again") {
        ZIO.scoped {
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
        }
      },
      test("An entity type stays registered until its entities have stopped") {
        ZIO.scoped {
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
        }
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

  // the stream restarts from where it ended when the entity restarts
  private def autoRestartTest(name: String)(
    autoRestart: Messenger[CounterMessage] => ZStream[Any, Any, Int]
  ): Spec[Sharding, Any] =
    test(name) {
      ZIO.scoped {
        for {
          counter <- startCounter()
          latch   <- Promise.make[Nothing, Unit]
          fiber   <- autoRestart(counter).tap(_ => latch.succeed(())).take(4).runCollect.fork
          _       <- latch.await
          _       <- counter.send("c1")(IncrementCounter())
          _       <- Sharding.terminateLocalEntity(Counter, "c1") // restarted from 0, the stream resumes from 2
          _       <- Clock.sleep(500.millis)
          _       <- counter.send("c1")(IncrementCounter())
          _       <- counter.send("c1")(IncrementCounter())
          _       <- counter.send("c1")(IncrementCounter())
          items   <- fiber.join
        } yield assertTrue(items == Chunk(0, 1, 2, 3))
      }
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
    case TriggerTerminate()                 extends CounterMessage[Nothing, Unit]
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
      sharding <- ZIO.service[Sharding]
      state    <- SubscriptionRef.make(0)
      _        <- ZIO.logInfo(s"Started entity $entityId")
    } yield new Counter.Handler {
      def handle[E, A](message: CounterMessage[E, A]): IO[E, A] =
        message match {
          case GetCounter()              => state.get
          case IncrementCounter()        => state.update(_ + 1)
          case DecrementCounter()        => state.update(_ - 1)
          case Fail()                    => ZIO.fail(CounterError("failed"))
          case Die()                     => ZIO.die(new Exception("boom"))
          case StreamingChanges(from)    => ZIO.succeed(Streamed(state.changes.filter(_ >= from).ensuring(state.set(-1))))
          case DyingStream()             =>
            ZIO.succeed(Streamed(ZStream.fromIterable(1 to 1000) ++ ZStream.die(new Exception("boom"))))
          case Chunked()                 => ZIO.succeed(Streamed(ZStream.fromChunks(Chunk(1, 2, 3), Chunk(4, 5))))
          case AddAndStreamChanges(from) =>
            ZIO.succeed(
              Duplex[Int, Nothing, Int](input =>
                state.changes
                  .filter(_ >= from)
                  .drainFork(input.mapZIO(n => state.update(_ + n)))
                  .ensuring(state.set(-1))
              )
            )
          case Doubled()                 => ZIO.succeed(Duplex[Int, Nothing, Int](_.map(_ * 2)))
          case TriggerTerminate()        => sharding.terminateLocalEntity(Counter, entityId)
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
  }

  object Worker extends EntityType[WorkerMessage]("worker")

  def startWorker(options: EntityOptions[WorkerMessage] = EntityOptions()): RIO[Sharding & Scope, WorkerState] =
    WorkerState.make.tap(state => Sharding.registerEntity(Worker, options)(worker(state)) *> Sharding.registerScoped)

  case class WorkerState(running: Ref[Int], finalized: Ref[Int], completed: Ref[Int])

  object WorkerState {
    val make: UIO[WorkerState] =
      (Ref.make(0) <*> Ref.make(0) <*> Ref.make(0)).map(WorkerState(_, _, _))
  }

  def worker(state: WorkerState)(entityId: String): URIO[Scope, Worker.Handler] =
    for {
      unblock <- Promise.make[Nothing, Unit]
      _       <- ZIO.addFinalizer(state.finalized.update(_ + 1))
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
        }
    }
}
