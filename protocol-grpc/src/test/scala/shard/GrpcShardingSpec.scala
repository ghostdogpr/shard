package shard

import shard.Messenger.MessengerTimeout
import shard.errors._
import shard.interfaces.{ MessageCodec, Storage }
import shard.interfaces.MessageCodec.Codec
import zio.stream.{ SubscriptionRef, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

/**
 * Sends messages over the real gRPC transport (`simulateRemotePods` forces it for the local pod).
 */
object GrpcShardingSpec extends ZIOSpecDefault {
  import GrpcShardingSpec.AccountActor._
  import GrpcShardingSpec.AccountActor.AccountMessage._

  private val config =
    ZLayer.succeed(Config.default.copy(shardingPort = 54329, simulateRemotePods = true, sendTimeout = 1.second))

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("GrpcShardingSpec")(
      test("Send, sendDiscard and typed failures") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            _       <- account.sendDiscard("a1")(Deposit(10))
            _       <- account.send("a1")(Deposit(5))
            error   <- account.send("a1")(Withdraw(100)).flip
            balance <- account.send("a1")(Balance())
          } yield assertTrue(error == NotEnoughFunds(15), balance == 15)
        }
      },
      test("Defects are received as EntityDefect") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            error   <- account.send("a1")(Crash()).flip
            items   <- account.stream("a1")(CrashingStream()).either.runCollect
          } yield assertTrue(
            error == EntityDefect("Entity died with java.lang.IllegalStateException: crash"),
            items == Chunk(Right(1), Left(EntityDefect("Entity died with java.lang.IllegalStateException: crash")))
          )
        }
      },
      test("Encoding and decoding failures are received as SerializationError, on either pod") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            message <- account.send("a1")(Unencodable()).flip
            discard <- account.sendDiscard("a1")(Unencodable()).flip
            decode  <- account.send("a1")(Undecodable()).flip
            reply   <- account.send("a1")(UnencodableReply()).flip
            encoded <- account.stream("a1")(Numbers(-1)).runCollect.flip
            decoded <- account.stream("a1")(Numbers(-3)).runCollect.flip
            input   <- account.duplex("a1")(DepositAll(), ZStream(1, -2)).runCollect.flip
          } yield assertTrue(
            message == SerializationError(
              "Failed to encode a message: java.lang.IllegalArgumentException: unencodable"
            ),
            discard == message,
            decode == SerializationError("Failed to decode a message: java.lang.IllegalArgumentException: undecodable"),
            reply == SerializationError("Failed to encode a reply: java.lang.IllegalArgumentException: unencodable"),
            encoded == SerializationError(
              "Failed to encode a stream element: java.lang.IllegalArgumentException: unencodable"
            ),
            decoded == SerializationError(
              "Failed to decode a stream element: java.lang.IllegalArgumentException: undecodable"
            ),
            input == SerializationError(
              "Failed to decode a stream input: java.lang.IllegalArgumentException: undecodable"
            )
          )
        }
      } @@ TestAspect.timeout(10.seconds),
      test("Unexpected transport failures are received as TransportError") {
        ZIO.scoped {
          for {
            _     <- Sharding.registerScoped
            // the entity type is not registered on the remote pod
            error <- Sharding.messenger(Account).flatMap(_.send("a1")(Balance())).flip
          } yield assertTrue(error match {
            case TransportError(message) => message.contains("INTERNAL") && message.contains("not registered")
            case _                       => false
          })
        }
      },
      test("Response streams, empty or cancelled when the entity stops") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            empty   <- account.stream("a1")(Empty()).runCollect
            latch   <- Promise.make[Nothing, Unit]
            fiber   <- account.stream("a1")(Changes()).tap(_ => latch.succeed(())).runCollect.either.fork
            _       <- latch.await
            _       <- account.send("a1")(Deposit(1))
            _       <- account.send("a1")(Deposit(2))
            _       <- Clock.sleep(200.millis)
            _       <- Sharding.terminateLocalEntity(Account, "a1")
            result  <- fiber.join
          } yield assertTrue(empty.isEmpty, result == Left(StreamCancelled))
        }
      },
      test("Stopping to consume a stream interrupts it on the remote pod") {
        ZIO.scoped {
          for {
            state   <- register()
            account <- Sharding.messenger(Account)
            items   <- account.stream("a1")(Changes()).take(1).runCollect
            _       <- Clock.sleep(500.millis)
            closed  <- state.closedStreams.get
          } yield assertTrue(items == Chunk(0), closed == 1)
        }
      },
      test("Duplex") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            items   <- account.duplex("a1")(DepositAll(), ZStream(1, 2, 3)).takeUntil(_ == 6).runCollect
          } yield assertTrue(items.last == 6)
        }
      },
      test("Send stream") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            _       <- account.sendStream("a1")(ZStream.fromIterable(1 to 100).as(Deposit(1)))
            balance <- account.send("a1")(Balance())
          } yield assertTrue(balance == 100)
        }
      },
      test("Timeout interrupts the message on the remote pod") {
        ZIO.scoped {
          for {
            state   <- register()
            account <- Sharding.messenger(Account, MessengerTimeout.Timeout(500.millis))
            error   <- account.send("a1")(Hang()).flip
            _       <- Clock.sleep(500.millis)
            hanging <- state.hanging.get
          } yield assertTrue(error == SendTimeout(Account.name, "a1"), hanging == 0)
        }
      },
      test("A messenger without timeout waits for slow messages") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account, MessengerTimeout.NoTimeout)
            _       <- account.send("a1")(Slow())
            balance <- account.send("a1")(Balance())
          } yield assertTrue(balance == 1)
        }
      },
      test("Messages sent while the pod doesn't host the entity are retried") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            // the pod doesn't start entities anymore
            _       <- Sharding.unregister
            send    <- account.send("a1")(Deposit(1)).fork
            stream  <- account.stream("a1")(Empty()).runCollect.fork
            sent    <- account.sendStream("a1")(ZStream(Deposit(1))).fork
            _       <- Clock.sleep(300.millis)
            pending <- ZIO.foreach(List(send, stream, sent))(_.poll)
            _       <- Sharding.register
            _       <- send.join
            items   <- stream.join
            _       <- sent.join
            balance <- account.send("a1")(Balance())
          } yield assertTrue(pending.forall(_.isEmpty), items.isEmpty, balance == 2)
        }
      },
      test("Mailbox full") {
        ZIO.scoped {
          for {
            _       <- register(EntityOptions(mailboxCapacity = 1))
            account <- Sharding.messenger(Account)
            _       <- account.sendDiscard("a1")(Hang())
            error   <- account.send("a1")(Balance()).flip
          } yield assertTrue(error == MailboxFull(Account.name, "a1"))
        }
      },
      test("A stream of messages fails when a message can't be encoded") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            error   <- account.sendStream("a1")(ZStream(Deposit(1), Unencodable(), Deposit(1))).flip
          } yield assertTrue(
            error == SerializationError("Failed to encode a message: java.lang.IllegalArgumentException: unencodable")
          )
        }
      } @@ TestAspect.timeout(10.seconds),
      test("A stream of messages dies when the stream dies") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            exit    <- account.sendStream("a1")(ZStream(Deposit(1)) ++ ZStream.die(new RuntimeException("boom"))).exit
          } yield assertTrue(exit.causeOption.flatMap(_.dieOption).exists(_.getMessage == "boom"))
        }
      } @@ TestAspect.timeout(10.seconds),
      test("A duplex fails when an input can't be encoded") {
        ZIO.scoped {
          for {
            _       <- register()
            account <- Sharding.messenger(Account)
            error   <- account.duplex("a1")(DepositAll(), ZStream(1, -1)).runCollect.flip
          } yield assertTrue(
            error == SerializationError(
              "Failed to encode a stream input: java.lang.IllegalArgumentException: unencodable"
            )
          )
        }
      } @@ TestAspect.timeout(10.seconds),
      test("Broadcast to an unavailable pod times out") {
        ZIO.scoped {
          for {
            state       <- register()
            _           <- Sharding.registerTopic(AccountTopic)(_ => behavior(state))
            unavailable  = PodAddress("localhost", 1)
            // a shard id that is not used, so that the pod only receives broadcasts
            _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map(301 -> Some(unavailable))))
            _           <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_.contains(unavailable))
            broadcaster <- Sharding.broadcaster(AccountTopic)
            replies     <- broadcaster.broadcast("t1")(Withdraw(1))
            self        <- ZIO.serviceWith[Config](config => PodAddress(config.selfHost, config.shardingPort))
            _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map.empty))
          } yield assertTrue(
            replies == Map(
              self        -> Exit.fail(NotEnoughFunds(0)),
              unavailable -> Exit.fail(SendTimeout(AccountTopic.name, "t1"))
            )
          )
        }
      } @@ TestAspect.timeout(10.seconds)
    ).provideShared(
      ShardManagerClient.local,
      Storage.memory,
      config,
      ZLayer.succeed(GrpcConfig.default),
      Sharding.live,
      GrpcPods.live,
      GrpcShardingService.live
    ) @@ sequential @@ withLiveClock

  object AccountActor {
    case class NotEnoughFunds(balance: Int)

    enum AccountMessage[+E, +A] {
      case Deposit(amount: Int)  extends AccountMessage[Nothing, Unit]
      case Withdraw(amount: Int) extends AccountMessage[NotEnoughFunds, Unit]
      case Balance()             extends AccountMessage[Nothing, Int]
      case Crash()               extends AccountMessage[Nothing, Unit]
      case Hang()                extends AccountMessage[Nothing, Unit]
      // takes longer than Config.sendTimeout
      case Slow()                extends AccountMessage[Nothing, Unit]
      case Changes()             extends AccountMessage[Nothing, Streamed[Nothing, Int]]
      case DepositAll()          extends AccountMessage[Nothing, Duplex[Int, Nothing, Int]]
      case Empty()               extends AccountMessage[Nothing, Streamed[Nothing, Int]]
      // fails to encode
      case Unencodable()         extends AccountMessage[Nothing, Unit]
      // fails to decode
      case Undecodable()         extends AccountMessage[Nothing, Unit]
      // replies with -1, which fails to encode
      case UnencodableReply()    extends AccountMessage[Nothing, Int]
      // 1, 2, n, 3
      case Numbers(n: Int)       extends AccountMessage[Nothing, Streamed[Nothing, Int]]
      // 1, then dies
      case CrashingStream()      extends AccountMessage[Nothing, Streamed[Nothing, Int]]
    }

    // Kryo, except for `Unencodable`, `Undecodable`, and the values -1 (fails to encode), -2 and -3 (fail to decode)
    given MessageCodec[AccountMessage] = {
      val kryo = shard.kryo.derive[AccountMessage]
      new MessageCodec[AccountMessage] {
        private def unencodable = throw new IllegalArgumentException("unencodable")
        private def undecodable = throw new IllegalArgumentException("undecodable")

        private def failing(codec: Codec): Codec =
          Codec(
            value => if (value == -1) unencodable else codec.encode(value),
            bytes => codec.decode(bytes) match { case -2 | -3 => undecodable; case value => value }
          )

        def encodeMessage(message: AccountMessage[Any, Any]): Array[Byte]          =
          message match {
            case Unencodable() => unencodable
            case other         => kryo.encodeMessage(other)
          }
        def decodeMessage(bytes: Array[Byte]): AccountMessage[Any, Any]            =
          kryo.decodeMessage(bytes) match {
            case Undecodable() => undecodable
            case other         => other
          }
        def replyCodec(message: AccountMessage[Any, Any]): MessageCodec.ReplyCodec = {
          val codec = kryo.replyCodec(message)
          codec.copy(value = failing(codec.value), streamInput = failing(codec.streamInput))
        }
      }
    }

    object Account      extends EntityType[AccountMessage]("account")
    object AccountTopic extends TopicType[AccountMessage]("account-topic")

    case class State(hanging: Ref[Int], closedStreams: Ref[Int])

    def register(options: EntityOptions[AccountMessage] = EntityOptions()): ZIO[Sharding & Scope, Throwable, State] =
      for {
        state <- (Ref.make(0) <*> Ref.make(0)).map(State(_, _))
        _     <- Sharding.registerEntity(Account, options)(_ => behavior(state))
        _     <- Sharding.registerScoped
      } yield state

    def behavior(state: State): UIO[Account.Handler] =
      SubscriptionRef.make(0).map { balance =>
        new Account.Handler {
          def handle[E, A](message: AccountMessage[E, A]): IO[E, A] =
            message match {
              case Deposit(amount)    => balance.update(_ + amount)
              case Withdraw(amount)   =>
                balance.get.flatMap(current =>
                  if (current < amount) ZIO.fail(NotEnoughFunds(current)) else balance.set(current - amount)
                )
              case Balance()          => balance.get
              case Slow()             => Clock.sleep(2.seconds) *> balance.update(_ + 1)
              case Crash()            => ZIO.die(new IllegalStateException("crash"))
              case Hang()             =>
                ZIO.acquireReleaseWith(state.hanging.update(_ + 1))(_ => state.hanging.update(_ - 1))(_ => ZIO.never)
              case Changes()          =>
                ZIO.succeed(Streamed(balance.changes.ensuring(state.closedStreams.update(_ + 1))))
              case Empty()            => ZIO.succeed(Streamed(ZStream.empty))
              case Unencodable()      => ZIO.unit
              case Undecodable()      => ZIO.unit
              case UnencodableReply() => ZIO.succeed(-1)
              case Numbers(n)         => ZIO.succeed(Streamed(ZStream(1, 2, n, 3)))
              case CrashingStream()   =>
                ZIO.succeed(Streamed(ZStream(1) ++ ZStream.die(new IllegalStateException("crash"))))
              case DepositAll()       =>
                ZIO.succeed(
                  Duplex[Int, Nothing, Int](input =>
                    balance.changes.drainFork(input.mapZIO(amount => balance.update(_ + amount)))
                  )
                )
            }
        }
      }
  }
}
