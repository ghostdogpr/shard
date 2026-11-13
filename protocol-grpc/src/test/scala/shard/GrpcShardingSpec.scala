package shard

import shard.BroadcastingSpec.IncrementerActor
import shard.BroadcastingSpec.IncrementerActor.{ Incrementer, IncrementerMessage }
import shard.Messenger.MessengerTimeout
import shard.errors._
import shard.interfaces.{ MessageCodec, Pods, Storage }
import shard.interfaces.MessageCodec.Codec
import shard.protocol.{ Sharding => ProtocolSharding }
import shard.protocol.Sharding.{ PingShardsRequest, PingShardsResponse }
import io.grpc.{ Attributes, Metadata }
import proteus.server.GrpcContext
import zio.stream.{ SubscriptionRef, ZStream }
import zio.test.TestAspect.{ sequential, withLiveClock }
import zio.test._
import zio.{ Config => _, _ }

object GrpcShardingSpec extends ZIOSpecDefault {
  import GrpcShardingSpec.AccountActor._
  import GrpcShardingSpec.AccountActor.AccountMessage._

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("GrpcShardingSpec")(
      overGrpc(Config.default.copy(shardingPort = 54330))(
        suite("ShardingSpec over gRPC")(ShardingSpec.tests.flatMap(_.filterNotTags(_ == ShardingSpec.inProcess))*)
      ),
      overGrpc(Config.default.copy(shardingPort = 54329, sendTimeout = 1.second))(
        suite("gRPC")(
          test("Encoding and decoding failures are received as SerializationError, on either pod") {
            for {
              account <- register
              message <- account.send("a1")(Unencodable()).flip
              discard <- account.sendDiscard("a1")(Unencodable()).flip
              sent    <- account.sendStream("a1")(ZStream(Deposit(1), Unencodable(), Deposit(1))).flip
              decode  <- account.send("a1")(Undecodable()).flip
              reply   <- account.send("a1")(UnencodableReply()).flip
              encoded <- account.stream("a1")(Numbers(-1)).runCollect.flip
              decoded <- account.stream("a1")(Numbers(-3)).runCollect.flip
              input   <- account.duplex("a1")(DepositAll(), ZStream(1, -2)).runCollect.flip
              encode  <- account.duplex("a1")(DepositAll(), ZStream(1, -1)).runCollect.flip
            } yield assertTrue(
              message == SerializationError(
                "Failed to encode a message: java.lang.IllegalArgumentException: unencodable"
              ),
              discard == message,
              sent == message,
              decode == SerializationError(
                "Failed to decode a message: java.lang.IllegalArgumentException: undecodable"
              ),
              reply == SerializationError(
                "Failed to encode a reply: java.lang.IllegalArgumentException: unencodable"
              ),
              encoded == SerializationError(
                "Failed to encode a stream element: java.lang.IllegalArgumentException: unencodable"
              ),
              decoded == SerializationError(
                "Failed to decode a stream element: java.lang.IllegalArgumentException: undecodable"
              ),
              input == SerializationError(
                "Failed to decode a stream input: java.lang.IllegalArgumentException: undecodable"
              ),
              encode == SerializationError(
                "Failed to encode a stream input: java.lang.IllegalArgumentException: unencodable"
              )
            )
          } @@ TestAspect.timeout(10.seconds),
          test("Composed server interceptors run in sequence order") {
            type Ping = PingShardsRequest
            type Pong = PingShardsResponse
            val pong: UIO[Pong]                   = ZIO.succeed(PingShardsResponse(None))
            val ctx                               = GrpcContext(
              new Metadata(),
              new Metadata(),
              None,
              ProtocolSharding.pingShards.toMethodDescriptor(ProtocolSharding.service),
              Attributes.EMPTY
            )
            def run(i: ShardingServerInterceptor) =
              i.unary[Ping, Pong](_ => pong).apply(PingShardsRequest())(ctx) *>
                i.clientStreaming[Ping, Pong](_ => _ => pong).apply(ZStream.empty)(ctx) *>
                i.serverStreaming[Ping, Pong](_ => ZStream.fromZIO(pong)).apply(PingShardsRequest())(ctx).runDrain *>
                i.bidiStreaming[Ping, Pong](_ => _ => ZStream.fromZIO(pong)).apply(ZStream.empty)(ctx).runDrain
            for {
              seen    <- Ref.make(Chunk.empty[String])
              composed =
                ShardingServerInterceptor.compose(
                  Seq("a", "b", "c").map(name => ShardingServerInterceptor.beforeEach(_ => seen.update(_ :+ name)))
                )
              _       <- run(composed)
              order   <- seen.get
            } yield assertTrue(order == Chunk.fill(4)(Chunk("a", "b", "c")).flatten)
          },
          test("A messenger without timeout waits for slow messages") {
            for {
              _       <- register
              account <- Sharding.messenger(Account, MessengerTimeout.NoTimeout)
              _       <- account.send("a1")(Slow())
              balance <- account.send("a1")(Balance())
            } yield assertTrue(balance == 1)
          },
          test("Broadcast to an unavailable pod times out") {
            for {
              _           <- Sharding.registerTopic(Incrementer)(IncrementerActor.behavior)
              _           <- Sharding.registerScoped
              unavailable  = PodAddress("localhost", 1)
              // a shard id that is not used, so that the pod only receives broadcasts
              _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map(301 -> Some(unavailable))))
              _           <- (Clock.sleep(10.millis) *> Sharding.getPods).repeatUntil(_.contains(unavailable))
              broadcaster <- Sharding.broadcaster(Incrementer)
              replies     <- broadcaster.broadcast("t1")(IncrementerMessage.Fail())
              self        <- ZIO.serviceWith[Config](config => PodAddress(config.selfHost, config.shardingPort))
              _           <- ZIO.serviceWithZIO[Storage](_.saveAssignments(Config.default.role, Map.empty))
            } yield assertTrue(
              replies == Map(
                self        -> Exit.fail("failed"),
                unavailable -> Exit.fail(SendTimeout(Incrementer.name, "t1"))
              )
            )
          } @@ TestAspect.timeout(10.seconds),
          test("Releasing GrpcPods closes its connections, including those opened from uninterruptible code or after") {
            for {
              self     <- ZIO.serviceWith[Config](config => PodAddress(config.selfHost, config.shardingPort))
              scope    <- Scope.make
              pods     <- scope.extend((ZLayer.succeed(GrpcConfig.default) >>> GrpcPods.live).build).map(_.get[Pods])
              _        <- Sharding.registerScoped
              current  <- ZIO.serviceWithZIO[Sharding](_.registration)
              pinged   <- pods.ping(self).uninterruptible
              other     = Role("other")
              assign   <- pods.assignShards(self, other, "r", Set.empty).exit
              unassign <- pods.unassignShards(self, other, "r", Set.empty).exit
              empty    <- pods.assignShards(self, Role(""), "r", Set.empty).exit
              stale    <- pods.assignShards(self, Role.default, "stale", Set.empty).exit
              closed   <- scope.close(Exit.unit).disconnect.timeout(5.seconds)
              // the same pod under another address, so that a new connection is opened
              late     <- pods.ping(self.copy(host = "127.0.0.1")).exit
            } yield assertTrue(
              pinged.isDefined,
              pinged == current,
              assign.isFailure,
              unassign.isFailure,
              empty.isFailure,
              stale.isFailure,
              closed.isDefined,
              late.isFailure
            )
          }
        )
      )
    ) @@ sequential @@ withLiveClock @@ TestAspect.timeout(60.seconds)

  private def overGrpc(config: Config)(spec: Spec[Sharding & Storage & Config & Scope, Any]): Spec[Scope, Any] =
    spec.provideSomeShared[Scope](
      ShardManagerClient.local,
      Storage.memory,
      // `simulateRemotePods` forces the gRPC transport for the local pod
      ZLayer.succeed(config.copy(simulateRemotePods = true)),
      ZLayer.succeed(GrpcConfig.default),
      Sharding.live,
      GrpcPods.live,
      GrpcShardingService.live
    )

  object AccountActor {
    enum AccountMessage[E, A] {
      case Deposit(amount: Int) extends AccountMessage[Nothing, Unit]
      case Balance()            extends AccountMessage[Nothing, Int]
      // takes longer than Config.sendTimeout
      case Slow()               extends AccountMessage[Nothing, Unit]
      case DepositAll()         extends AccountMessage[Nothing, Duplex[Int, Nothing, Int]]
      // fails to encode
      case Unencodable()        extends AccountMessage[Nothing, Unit]
      // fails to decode
      case Undecodable()        extends AccountMessage[Nothing, Unit]
      // replies with -1, which fails to encode
      case UnencodableReply()   extends AccountMessage[Nothing, Int]
      // 1, 2, n, 3
      case Numbers(n: Int)      extends AccountMessage[Nothing, Streamed[Nothing, Int]]
    }

    // Java serialization that fails for `Unencodable`, `Undecodable` and the values -1, -2 and -3
    given MessageCodec[AccountMessage] = {
      val base = shard.javaSerialization.derive[AccountMessage]
      new MessageCodec[AccountMessage] {
        private def unencodable = throw new IllegalArgumentException("unencodable")
        private def undecodable = throw new IllegalArgumentException("undecodable")

        private def failing(codec: Codec): Codec =
          Codec(
            value => if (value == -1) unencodable else codec.encode(value),
            bytes => codec.decode(bytes) match { case -2 | -3 => undecodable; case value => value }
          )

        def encodeMessage[E, A](message: AccountMessage[E, A]): Array[Byte]          =
          message match {
            case Unencodable() => unencodable
            case other         => base.encodeMessage(other)
          }
        def decodeMessage(bytes: Array[Byte]): AccountMessage[Any, Any]              = {
          val message = base.decodeMessage(bytes)
          if (message.isInstanceOf[Undecodable]) undecodable else message
        }
        def replyCodec[E, A](message: AccountMessage[E, A]): MessageCodec.ReplyCodec = {
          val codec = base.replyCodec(message)
          codec.copy(value = failing(codec.value), streamInput = failing(codec.streamInput))
        }
      }
    }

    object Account extends EntityType[AccountMessage]("account")

    val register: RIO[Sharding & Scope, Messenger[AccountMessage]] =
      Sharding.registerEntity(Account)(_ => behavior) *> Sharding.registerScoped *> Sharding.messenger(Account)

    val behavior: UIO[Account.Handler] =
      SubscriptionRef.make(0).map { balance =>
        new Account.Handler {
          def handle[E, A](message: AccountMessage[E, A]): IO[E, A] =
            message match {
              case Deposit(amount)    => balance.update(_ + amount)
              case Balance()          => balance.get
              case Slow()             => Clock.sleep(2.seconds) *> balance.update(_ + 1)
              case Unencodable()      => ZIO.unit
              case Undecodable()      => ZIO.unit
              case UnencodableReply() => ZIO.succeed(-1)
              case Numbers(n)         => ZIO.succeed(Streamed(ZStream(1, 2, n, 3)))
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
