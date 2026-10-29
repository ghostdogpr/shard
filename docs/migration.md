# Migrating from Shardcake

Shard 3.0 is the successor of [Shardcake](https://github.com/devsisters/shardcake) 2.x. Most concepts are unchanged (entities, topics, singletons, shards, roles, the Shard Manager), but the messaging API is now typed, and the protocol between pods and with the Shard Manager has changed.

This section lists what to change in your code, then how to upgrade a running cluster.

## Dependencies and packages

The organization, the artifacts and the package were renamed:
- `"com.devsisters" %% "shardcake-xxx"` becomes `"com.github.ghostdogpr" %% "shard-xxx"` (e.g. `shard-entities`, `shard-manager`, `shard-protocol-grpc`).
- The package `com.devsisters.shardcake` becomes `shard`.

Shard requires Scala 3.9 LTS or later: Scala 2.13 is no longer supported.

## Messages

In Shardcake, a message that expects a reply carries a `Replier[A]`, and errors are usually returned as part of the reply (e.g. with a `Try`).
In Shard, the message type has two type parameters: each message declares the type `E` of the errors it can fail with and the type `A` of its reply. `Replier` and `StreamReplier` are gone.

Before:
```scala
sealed trait GuildMessage

object GuildMessage {
  case class Join(userId: String, replier: Replier[Try[Set[String]]]) extends GuildMessage
  case class Leave(userId: String)                                    extends GuildMessage
}

object Guild extends EntityType[GuildMessage]("guild")
```

After:
```scala
case class GuildFull(maxMembers: Int)

enum GuildMessage[E, A] {
  case Join(userId: String)  extends GuildMessage[GuildFull, Set[String]]
  case Leave(userId: String) extends GuildMessage[Nothing, Unit]
}

object Guild extends EntityType[GuildMessage]("guild")
```

A message that replied with a `StreamReplier[A]` now declares a `Streamed[E2, A]` reply type (see [Streaming](entities.md#streaming)).

## Serialization

The `Serialization` layer is replaced by a `MessageCodec[Msg]` given, required where the entity or topic type is declared. Instead of providing `KryoSerialization.live`, import the Kryo given:
```scala
import shard.kryo.given

object Guild extends EntityType[GuildMessage]("guild")
```
Remove `Serialization` from the layers given to `Sharding.live`. See [Message Codec](customization.md#message-codec) for the other backends, including Protobuf.

## Behaviors

In Shardcake, the behavior takes a `Queue` of messages and consumes it forever, replying through the `Replier` of each message.
In Shard, the behavior is called when an entity starts and returns a **handler**, whose `handle` method returns the reply of each message. Failing with `E` is how a message replies with an error.

Before:
```scala
def behavior(entityId: String, messages: Queue[GuildMessage]): RIO[Sharding, Nothing] =
  Ref.make(Set.empty[String]).flatMap { state =>
    messages.take.flatMap {
      case Join(userId, replier) =>
        state.get.flatMap(members =>
          if (members.size >= 5) replier.reply(Failure(new Exception("Guild is full")))
          else state.updateAndGet(_ + userId).flatMap(members => replier.reply(Success(members)))
        )
      case Leave(userId)         =>
        state.update(_ - userId)
    }.forever
  }
```

After:
```scala
def behavior(entityId: String): ZIO[Scope, Nothing, Guild.Handler] =
  Ref.make(Set.empty[String]).map { state =>
    new Guild.Handler {
      def handle[E, A](message: GuildMessage[E, A]): IO[E, A] =
        message match {
          case Join(userId)  =>
            state.get.flatMap(members =>
              if (members.size >= 5) ZIO.fail(GuildFull(5))
              else state.updateAndGet(_ + userId)
            )
          case Leave(userId) =>
            state.update(_ - userId)
        }
    }
  }
```

A message can no longer be left without a reply: the sender always gets the result of `handle`, or `EntityDefect` if it died.

Some patterns of the queue-based behaviors have a different equivalent:
- `terminateMessage` is replaced by the `Scope` given to the behavior: use `ZIO.addFinalizer` to save the entity state when it stops. The finalizers run after the last message was processed, within `entityTerminationTimeout`.
- Messages were processed one at a time unless the behavior forked them. Use the `concurrency` option to process several messages at the same time, or the `Concurrent` marker trait for messages that can run alongside the others (see [Messages](entities.md#messages)).
- Work running in the background of an entity (timers, subscriptions) can be forked in its scope with `forkScoped`: it is interrupted when the entity stops.

## Registration

The optional parameters of `registerEntity` and `registerTopic` move to `EntityOptions`, and the behavior is given in a second parameter list:
- `entityMaxIdleTime` becomes `EntityOptions.maxIdleTime`.
- `loadEntity` becomes `EntityOptions.loadEntity`. It is a polymorphic function, since messages have type parameters.
- `terminateMessage` is gone (see above).

```scala
// before
Sharding.registerEntity(Guild, behavior, entityMaxIdleTime = Some(5.minutes))

// after
Sharding.registerEntity(Guild, EntityOptions(maxIdleTime = Some(5.minutes)))(behavior)
```

`EntityOptions` also configures the new `concurrency`, `mailboxCapacity` and `interceptor` options (see [Entity options](entities.md#entity-options)).

## Sending messages

Messages are sent without the `Replier` callback, and the `Messenger` methods were renamed:

| Shardcake 2.x                                 | Shard 3.0                                      |
| --------------------------------------------- | ---------------------------------------------- |
| `send(id)(Join("user1", _))`                  | `send(id)(Join("user1"))`                      |
| `sendDiscard(id)(msg)`                        | `sendDiscard(id)(msg)`                         |
| `sendAndReceiveStream(id)(Msg(_))`            | `stream(id)(Msg())`                            |
| `sendStreamAndReceiveStream(id)(r => stream)` | `duplex(id)(Msg(), inputs)`                    |
| `sendStream(id)(messages)`                    | `sendStream(id)(messages)`                     |
| `sendAndReceiveStreamAutoRestart`             | `streamAutoRestart`                            |
| `sendStreamAndReceiveStreamAutoRestart`       | `duplexAutoRestart`                            |
| `broadcast(topic)(Msg(_))`                    | `broadcast(topic)(Msg())`                      |

A few behaviors changed:
- Errors are typed: `send` returns an `IO[E | ClusterError, A]` instead of a `Task`. `ClusterError` covers the errors of the cluster itself (see [Errors](entities.md#errors)), e.g. `SendTimeoutException` is now `SendTimeout`.
- `stream` and `duplex` return a `ZStream` directly, instead of a `Task` of a stream.
- `duplex` sends a single message along with a stream of inputs, instead of a stream of messages that each share the same `StreamReplier`.
- `sendStream` only takes messages that reply with nothing (`Msg[Nothing, Unit]`).
- `broadcast` returns a `Map[PodAddress, Exit[E | ClusterError, A]]` instead of a `Map[PodAddress, Try[A]]`.
- When the processing of a message times out, it is interrupted (unless it is `Uninterruptible`).
- When an entity is full, messages fail with `MailboxFull` (see `mailboxCapacity`).

## Configuration

On pods, `Config.shardManagerUri` is replaced by `shardManagerHost`, `shardManagerPort` and `shardManagerTimeout`, since the Shard Manager API now uses gRPC instead of GraphQL.
`ShardManagerClient.liveWithSttp` becomes `ShardManagerClient.live`, which requires a `GrpcConfig`.

```scala
// before
Config.default.copy(shardManagerUri = uri"http://shard-manager:8080/api/graphql")

// after
Config.default.copy(shardManagerHost = "shard-manager", shardManagerPort = 8080)
```

`Config` also has a new `role` (see [Configuration](config.md#sharding-configuration)) and `entityInterceptor`.

## Shard Manager

- `Server.run()` becomes `Server.run`, and requires a `GrpcConfig`. It serves a gRPC API on `apiPort` instead of a GraphQL API over HTTP, along with the standard gRPC health service. Kubernetes probes should use `grpc` instead of `httpGet` on `/health`.
- `ManagerConfig.numberOfShards` is a function `Role => Int`. Use `_ => 300` if you don't use roles.
- Tools that called the GraphQL API can use the gRPC API instead, or `Sharding.getAssignments` and `Sharding.getRegisteredPods` from a pod.

## Storage

The Redis storage has a new default implementation, `shard-storage-sage`, which uses the [Sage](https://ghostdogpr.github.io/sage/) client. The redis4cats (`shard-storage-redis`) and Redisson (`shard-storage-redisson`) implementations are still available (see [Storage](customization.md#storage)).

`RedisConfig.assignmentsKey` is now a function `Role => String`, since each role has its own assignments. The format of the stored pods changed, so Shardcake and Shard can't share the same data (see [below](#upgrading-a-running-cluster)).

## Custom implementations

If you implemented the pluggable interfaces yourself:
- `Storage` methods for assignments take a `Role`, and `Pod` has a `role`.
- `Pods` sends `BinaryMessage` and receives `BinaryReply`, which carries the outcome of the message (success, typed failure, defect, interruption, etc.) instead of optional bytes.
- `Serialization` is replaced by `MessageCodec[Msg]`, which also encodes the replies and errors of each message.

## Metrics

Metrics are prefixed with `shard.` instead of `shardcake.` (e.g. `shard.entities`), and those that depend on a role are tagged with `role`. Update your dashboards and alerts accordingly (see [Metrics](metrics.md)).

## Upgrading a running cluster

Shardcake pods and Shard pods can't be part of the same cluster: the protocol between pods, the Shard Manager API and the stored data are all different.
A rolling update from Shardcake to Shard is therefore not possible. Instead, run the new version as a separate cluster next to the old one (a **blue/green** deployment), then switch to it.

The new cluster needs:
- Its own Shard Manager, with a different address than the old one. Point the new pods to it with `shardManagerHost` and `shardManagerPort`.
- Its own storage. Either use a separate Redis instance (or database), or use different keys: the assignment keys are different by default (`shard_assignments` and `shard_assignments:<role>`), but both versions use `pods` to store pods and overwrite it entirely, so the two Shard Managers would erase each other's pods. Set a different `podsKey` in the `RedisConfig` of the new Shard Manager and pods.
- Every pod that sends messages to the entities, not only those that host them, since a pod can only send messages to the entities of its own cluster.

As long as both clusters run, an entity can be running in both of them, and a singleton runs once in each. Choose how to switch depending on whether that is acceptable:
- **Switch with an overlap**: deploy the new cluster, move the traffic to it, then stop the old one. This has no downtime, but an entity started in the new cluster can run while the old one is still running and saving its state, so it may load a state that is not up to date.
- **Switch with a pause**: deploy the new cluster without traffic, stop sending messages to the old one, stop the old pods so that their entities finish their messages and run their finalizers, then move the traffic to the new cluster. Entities always start from their last saved state, at the cost of a short pause. Keep singletons from doing anything in the new cluster before the switch, since they start as soon as its pods are registered.

Keep the old cluster deployable until the switch is done, to be able to move the traffic back to it.

::: tip Next upgrades
Once on Shard, rolling updates work again between Shard versions, as long as your messages remain compatible (see the tip in [Message Codec](customization.md#message-codec)).
:::
