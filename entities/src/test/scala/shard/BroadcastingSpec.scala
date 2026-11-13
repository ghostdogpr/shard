package shard

import shard.Messenger.MessengerTimeout
import shard.interfaces.Storage
import shard.internal.LocalShardManagerClient
import shard.javaSerialization.given
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object BroadcastingSpec extends ZIOSpecDefault {

  private val config = ZLayer.succeed(Config.default.copy(simulateRemotePods = true))

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("BroadcastingSpec")(
      test("Send broadcast to entities") {
        for {
          _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
          _           <- Sharding.registerScoped
          incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer)
          // returns once the message is in the mailbox, so it is processed before the next one
          _           <- incrementer.broadcastDiscard("c1")(IncrementerActor.IncrementerMessage.BroadcastIncrement())
          // a topic instance is not tied to the shard of its name, so it survives its unassignment
          sharding    <- ZIO.service[Sharding]
          shard        = sharding.getShardId(IncrementerActor.Incrementer, "c1")
          _           <- sharding.unassignShards(Set(shard)).ensuring(sharding.assignShards(Set(shard)))
          c1          <- incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.GetIncrement())
        } yield assertTrue(
          // Here we have just one pod, so there will be just one incrementer
          c1.values.toList == List(Exit.succeed(1))
        )
      },
      test("Broadcast to several pods, with a typed failure") {
        for {
          _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
          _           <- Sharding.registerScoped
          self        <- ZIO.serviceWith[Config](config => PodAddress(config.selfHost, config.shardingPort))
          // another pod on an unused shard (the local transport delivers its messages locally)
          other        = PodAddress("other", 1)
          _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map(301 -> Some(other))))
          _           <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_.contains(other))
          incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer)
          replies     <- incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.Fail())
          _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map.empty))
        } yield assertTrue(replies == Map(self -> Exit.fail("failed"), other -> Exit.fail("failed")))
      },
      test("A broadcast without a timeout can be interrupted, even from an uninterruptible region") {
        for {
          _           <- Sharding.registerTopic(IncrementerActor.Incrementer)(IncrementerActor.behavior)
          _           <- Sharding.registerScoped
          incrementer <- Sharding.broadcaster(IncrementerActor.Incrementer, MessengerTimeout.NoTimeout)
          fiber       <- ZIO.uninterruptible(incrementer.broadcast("c1")(IncrementerActor.IncrementerMessage.Hang())).fork
          stopped     <- fiber.interrupt.timeout(5.seconds)
        } yield assertTrue(stopped.isDefined)
      }
    ).provideSomeShared[Scope](
      LocalSharding.live,
      LocalShardManagerClient.live,
      Storage.memory,
      config
    ) @@ sequential @@ withLiveClock

  object IncrementerActor {
    enum IncrementerMessage[E, A] {
      case BroadcastIncrement() extends IncrementerMessage[Nothing, Unit]
      case GetIncrement()       extends IncrementerMessage[Nothing, Int]
      case Fail()               extends IncrementerMessage[String, Unit]
      case Hang()               extends IncrementerMessage[Nothing, Unit]
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
                  case IncrementerMessage.Hang()               => ZIO.never
                }
            }
          )
  }
}
