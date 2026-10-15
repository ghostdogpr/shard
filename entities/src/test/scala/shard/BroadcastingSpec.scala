package shard

import shard.errors.EntityDefect
import shard.interfaces.Storage
import shard.javaSerialization.given
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object BroadcastingSpec extends ZIOSpecDefault {

  private val config = ZLayer.succeed(Config.default.copy(simulateRemotePods = true))

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("BroadcastingSpec")(
      test("Send broadcast to entities") {
        ZIO.scoped {
          for {
            _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
            _           <- Sharding.registerScoped
            incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer)
            _           <- incrementer.broadcastDiscard("c1")(IncrementerActor.IncrementerMessage.BroadcastIncrement())
            _           <- Clock.sleep(1 second)
            c1          <- incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.GetIncrement())
          } yield assertTrue(
            c1.values.toList == List(
              Exit.succeed(1)
            ) // Here we have just one pod, so there will be just one incrementer
          )
        }
      },
      test("Broadcast to several pods, with a typed failure") {
        ZIO.scoped {
          for {
            _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
            _           <- Sharding.registerScoped
            self        <- ZIO.serviceWith[Config](config => PodAddress(config.selfHost, config.shardingPort))
            // another pod, assigned a shard id that is not used: the local transport delivers its messages locally too
            other        = PodAddress("other", 1)
            _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map(301 -> Some(other))))
            _           <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_.contains(other))
            incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer)
            replies     <- incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.Fail())
            _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map.empty))
          } yield assertTrue(replies == Map(self -> Exit.fail("failed"), other -> Exit.fail("failed")))
        }
      },
      test("Broadcast to entities that die") {
        ZIO.scoped {
          for {
            _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
            _           <- Sharding.registerScoped
            incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer)
            replies     <- incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.Die())
          } yield assertTrue(
            replies.values.toList == List(Exit.fail(EntityDefect("Entity died with java.lang.Exception: boom")))
          )
        }
      }
    ).provideShared(
      LocalSharding.live,
      ShardManagerClient.local,
      Storage.memory,
      config
    ) @@ sequential @@ withLiveClock

  object IncrementerActor {
    enum IncrementerMessage[+E, +A] {
      case BroadcastIncrement() extends IncrementerMessage[Nothing, Unit]
      case GetIncrement()       extends IncrementerMessage[Nothing, Int]
      case Fail()               extends IncrementerMessage[String, Unit]
      case Die()                extends IncrementerMessage[Nothing, Unit]
    }

    object Incrementer extends TopicType[IncrementerMessage]("incrementer")

    def behavior(topic: String): UIO[Incrementer.Handler] =
      ZIO.debug(s"Started topic $topic on this pod") *>
        Ref
          .make(0)
          .map(ref =>
            new Incrementer.Handler {
              def handle[E, A](message: IncrementerMessage[E, A]): IO[E, A] =
                message match {
                  case IncrementerMessage.BroadcastIncrement() => ref.update(_ + 1)
                  case IncrementerMessage.GetIncrement()       => ref.get
                  case IncrementerMessage.Fail()               => ZIO.fail("failed")
                  case IncrementerMessage.Die()                => ZIO.die(new Exception("boom"))
                }
            }
          )
  }
}
