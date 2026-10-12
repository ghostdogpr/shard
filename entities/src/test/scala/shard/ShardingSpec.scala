package shard

import shard.CounterActor.CounterMessage._
import shard.CounterActor._
import shard.StubbornActor.{ Stubborn, StubbornMessage }
import shard.interfaces.Storage
import shard.javaSerialization.given
import zio.stream.{ SubscriptionRef, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object ShardingSpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ShardingSpec")(
      test("Send message to entities") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            _       <- counter.sendDiscard("c1")(DecrementCounter)
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            _       <- counter.sendDiscard("c2")(IncrementCounter)
            _       <- Clock.sleep(1 second)
            c1      <- counter.send("c1")(GetCounter.apply)
            c2      <- counter.send("c2")(GetCounter.apply)
          } yield assertTrue(c1 == 2, c2 == 1)
        }
      },
      test("Response stream") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            stream  <- counter.sendAndReceiveStream("c1")(StreamingChanges.apply)
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- stream.take(5).tap(_ => latch.succeed(())).runCollect.fork
            _       <- latch.await
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            _       <- counter.sendDiscard("c1")(DecrementCounter)
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            _       <- counter.sendDiscard("c1")(IncrementCounter)
            items   <- fiber.join
          } yield assertTrue(items == Chunk(0, 1, 0, 1, 2))
        }
      },
      test("Response stream interruption") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            stream  <- counter.sendAndReceiveStream("c1")(StreamingChanges.apply)
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- stream.take(2).tap(_ => latch.succeed(())).runCollect.fork
            _       <- latch.await
            _       <- fiber.interrupt
            _       <- Clock.sleep(1 second)
            res     <- counter.send("c1")(GetCounter.apply)
          } yield assertTrue(res == -1)
        }
      },
      test("Entity termination") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior, entityMaxIdleTime = Some(1.seconds))
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            _       <- counter.sendDiscard("c3")(IncrementCounter)
            c0      <- counter.send("c3")(GetCounter.apply)
            _       <- Clock.sleep(3 seconds)
            c1      <- counter.send("c3")(GetCounter.apply) // counter should be restarted
          } yield assertTrue(c0 == 1, c1 == 0)
        }
      },
      test("Entity manual termination") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            _       <- counter.sendDiscard("c3")(IncrementCounter)
            c0      <- counter.send("c3")(GetCounter.apply)
            _       <- counter.send("c3")(TriggerTerminate.apply)
            c1      <- counter.send("c3")(GetCounter.apply) // counter should be restarted
          } yield assertTrue(c0 == 1, c1 == 0)
        }
      },
      test("Entity termination with extension") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior, entityMaxIdleTime = Some(3.seconds))
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            _       <- counter.sendDiscard("c3")(IncrementCounter)
            c0      <- counter.send("c3")(GetCounter.apply)
            _       <- Clock.sleep(2 seconds)
            _       <- counter.sendDiscard("c3")(IncrementCounter)
            c1      <- counter.send("c3")(GetCounter.apply)
            _       <- Clock.sleep(4 seconds)
            c2      <- counter.send("c3")(GetCounter.apply) // counter should be restarted
          } yield assertTrue(c0 == 1, c1 == 2, c2 == 0)
        }
      },
      test("Entity ignoring its termination message is interrupted after the timeout") {
        ZIO.scoped {
          for {
            running  <- Ref.make(0)
            _        <- Sharding.registerEntity(
                          Stubborn,
                          StubbornActor.behavior(running),
                          p => Some(StubbornMessage.Terminate(p)),
                          entityMaxIdleTime = Some(1.second)
                        )
            _        <- Sharding.registerScoped
            stubborn <- Sharding.messenger(Stubborn)
            _        <- stubborn.send("s1")(StubbornMessage.Ping.apply)
            before   <- running.get
            // 1s of idle time, then 3s of termination timeout (Config.default)
            _        <- Clock.sleep(7.seconds)
            after    <- running.get
          } yield assertTrue(before == 1, after == 0)
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
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            _       <- counter.sendStream("c1")(ZStream.fromIterable(1 to 10000).as(IncrementCounter))
            c0      <- counter.send("c1")(GetCounter.apply)
          } yield assertTrue(c0 == 10000)
        }
      },
      test("Send stream and receive a stream") {
        ZIO.scoped {
          for {
            _       <- Sharding.registerEntity(Counter, behavior)
            _       <- Sharding.registerScoped
            counter <- Sharding.messenger(Counter)
            stream  <- counter.sendStreamAndReceiveStream[Int]("c1")(
                         StreamingChanges.apply,
                         ZStream.fromIterable(1 to 5).as(IncrementCounter)
                       )
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- stream.take(5).tap(_ => latch.succeed(())).runCollect.fork
            _       <- latch.await
            _       <- fiber.interrupt
            _       <- Clock.sleep(3 seconds)
            res     <- counter.send("c1")(GetCounter.apply)
          } yield assertTrue(res == -1)
        }
      }
    ).provideShared(
      LocalSharding.live,
      ShardManagerClient.local,
      Storage.memory,
      ZLayer.succeed(Config.default)
    ) @@ sequential @@ withLiveClock
}

object CounterActor {
  sealed trait CounterMessage

  object CounterMessage {
    case class GetCounter(replier: Replier[Int])             extends CounterMessage
    case object IncrementCounter                             extends CounterMessage
    case object DecrementCounter                             extends CounterMessage
    case class StreamingChanges(replier: StreamReplier[Int]) extends CounterMessage
    case class TriggerTerminate(replier: Replier[Unit])      extends CounterMessage
  }

  object Counter extends EntityType[CounterMessage]("counter")

  def behavior(entityId: String, messages: Dequeue[CounterMessage]): RIO[Sharding, Nothing] =
    ZIO.logInfo(s"Started entity $entityId") *>
      SubscriptionRef
        .make(0)
        .flatMap(state =>
          messages.take.flatMap {
            case CounterMessage.GetCounter(replier)       => state.get.flatMap(replier.reply)
            case CounterMessage.IncrementCounter          => state.update(_ + 1)
            case CounterMessage.DecrementCounter          => state.update(_ - 1)
            case CounterMessage.StreamingChanges(replier) =>
              replier.replyStream(state.changes.ensuring(state.set(-1)))
            case CounterMessage.TriggerTerminate(replier) =>
              Sharding.terminateLocalEntity(Counter, entityId).ensuring(replier.reply(()))
          }.forever
        )
}

object StubbornActor {
  sealed trait StubbornMessage

  object StubbornMessage {
    case class Ping(replier: Replier[Unit])         extends StubbornMessage
    case class Terminate(p: Promise[Nothing, Unit]) extends StubbornMessage
  }

  object Stubborn extends EntityType[StubbornMessage]("stubborn")

  // never completes its termination signal, so it only stops when interrupted
  def behavior(running: Ref[Int])(entityId: String, messages: Dequeue[StubbornMessage]): RIO[Sharding, Nothing] =
    ZIO.acquireReleaseWith(running.update(_ + 1))(_ => running.update(_ - 1)) { _ =>
      messages.take.flatMap {
        case StubbornMessage.Ping(replier) => replier.reply(())
        case StubbornMessage.Terminate(_)  => ZIO.unit
      }.forever
    }
}
