package shard

import shard.errors.EntityDefect
import shard.interfaces.Storage
import shard.proteus.given
import zio.stream.{ SubscriptionRef, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

/**
 * End-to-end test that exercises the full Sharding + LocalSharding flow using the Proteus
 * backend. Reply types are case classes (Proteus only supports messages / enums at the
 * root, not primitives), so we wrap `Int` in a small `Count` case class.
 *
 * The local-pod shortcut means most of the action goes through the typed path;
 * setting `simulateRemotePods = true` forces the binary path through Proteus.
 */
object ProteusShardingSpec extends ZIOSpecDefault {
  import ProteusShardingSpec.CounterActor._
  import ProteusShardingSpec.CounterActor.CounterMessage._

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ProteusShardingSpec")(
      suite("local-pod shortcut")(tests*).provideShared(
        LocalSharding.live,
        ShardManagerClient.local,
        Storage.memory,
        ZLayer.succeed(Config.default)
      ),
      suite("simulate remote pods (forces proteus serialization on every hop)")(tests*).provideShared(
        LocalSharding.live,
        ShardManagerClient.local,
        Storage.memory,
        ZLayer.succeed(Config.default.copy(simulateRemotePods = true))
      )
    ) @@ sequential @@ withLiveClock

  private val tests: List[Spec[Sharding, Any]] = List(
    test("Send fire-and-forget then ask for the count") {
      ZIO.scoped {
        for {
          counter <- startCounter
          _       <- counter.sendDiscard("c1")(Increment())
          _       <- counter.sendDiscard("c1")(Increment())
          _       <- counter.send("c1")(Decrement())
          _       <- counter.send("c2")(Increment())
          c1      <- counter.send("c1")(Get())
          c2      <- counter.send("c2")(Get())
        } yield assertTrue(c1.value == 1, c2.value == 1)
      }
    },
    test("Typed failure over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          error   <- counter.send("c1")(Withdraw(5)).flip
          c1      <- counter.send("c1")(Get())
        } yield assertTrue(error == CounterError.NotEnough(0), c1.value == 0)
      }
    },
    test("Defect over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          error   <- counter.send("c1")(Crash()).flip
        } yield assertTrue(error == EntityDefect("Entity died with java.lang.IllegalStateException: crash"))
      }
    },
    test("Response stream over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          latch   <- Promise.make[Nothing, Unit]
          fiber   <- counter.stream("c1")(Changes()).take(4).tap(_ => latch.succeed(())).runCollect.fork
          _       <- latch.await
          _       <- counter.sendDiscard("c1")(Increment())
          _       <- counter.sendDiscard("c1")(Increment())
          _       <- counter.sendDiscard("c1")(Decrement())
          items   <- fiber.join
        } yield assertTrue(items.map(_.value) == Chunk(0, 1, 2, 1))
      }
    },
    test("Response stream failures over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          before  <- counter.stream("c1")(CountDown(-1)).runCollect.flip
          during  <- counter.stream("c1")(CountDown(2)).runCollect.either
        } yield assertTrue(
          before == CounterError.Negative(),
          during == Left(CounterError.Exhausted(Count(0)))
        )
      }
    },
    test("Duplex over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          items   <- counter.duplex("c1")(Add(), ZStream(Count(1), Count(2), Count(3))).takeUntil(_.value == 6).runCollect
        } yield assertTrue(items.last.value == 6)
      }
    },
    test("Send stream over Proteus") {
      ZIO.scoped {
        for {
          counter <- startCounter
          _       <- counter.sendStream("c1")(ZStream.fromIterable(1 to 100).as(Increment()))
          c1      <- counter.send("c1")(Get())
        } yield assertTrue(c1.value == 100)
      }
    }
  )

  // Reply payload wrapped in a case class — Proteus needs a message at the root, not a primitive.
  final case class Count(value: Int)

  sealed trait CounterError
  object CounterError {
    final case class NotEnough(available: Int) extends CounterError
    final case class Negative()                extends CounterError
    final case class Exhausted(last: Count)    extends CounterError
  }

  object CounterActor {
    enum CounterMessage[+E, +A] {
      case Get()                 extends CounterMessage[Nothing, Count]
      case Increment()           extends CounterMessage[Nothing, Unit]
      case Decrement()           extends CounterMessage[Nothing, Unit]
      case Withdraw(amount: Int) extends CounterMessage[CounterError, Unit]
      case Crash()               extends CounterMessage[Nothing, Unit]
      case Changes()             extends CounterMessage[Nothing, Streamed[Nothing, Count]]
      // counts down to 0 then fails, or fails right away if negative
      case CountDown(from: Int)  extends CounterMessage[CounterError.Negative, Streamed[CounterError.Exhausted, Count]]
      case Add()                 extends CounterMessage[Nothing, Duplex[Count, Nothing, Count]]
    }

    object Counter extends EntityType[CounterMessage]("counter")

    def startCounter: RIO[Sharding & Scope, Messenger[CounterMessage]] =
      Sharding.registerEntity(Counter)(behavior) *> Sharding.registerScoped *> Sharding.messenger(Counter)

    def behavior(entityId: String): UIO[Counter.Handler] =
      SubscriptionRef.make(0).map { state =>
        new Counter.Handler {
          def handle[E, A](message: CounterMessage[E, A]): IO[E, A] =
            message match {
              case Get()            => state.get.map(Count(_))
              case Increment()      => state.update(_ + 1)
              case Decrement()      => state.update(_ - 1)
              case Withdraw(amount) =>
                state.get.flatMap(available =>
                  if (available < amount) ZIO.fail(CounterError.NotEnough(available)) else state.update(_ - amount)
                )
              case Crash()          => ZIO.die(new IllegalStateException("crash"))
              case Changes()        => ZIO.succeed(Streamed(state.changes.map(Count(_))))
              case CountDown(from)  =>
                if (from < 0) ZIO.fail(CounterError.Negative())
                else
                  ZIO.succeed(
                    Streamed(
                      ZStream.range(0, from + 1).map(n => Count(from - n)) ++ ZStream
                        .fail(CounterError.Exhausted(Count(0)))
                    )
                  )
              case Add()            =>
                ZIO.succeed(
                  Duplex[Count, Nothing, Count](input =>
                    state.changes.map(Count(_)).drainFork(input.mapZIO(count => state.update(_ + count.value)))
                  )
                )
            }
        }
      }
  }
}
