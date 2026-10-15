# Entities and Messaging

This section details how to define entities and how to send them messages. It builds on the guild example of the [Getting Started](getting-started.md) section.

## Messages

The messages of an entity type are an `enum` (or a `sealed trait`) with two type parameters: for each message, `E` is the type of the errors it can fail with and `A` is the type of its reply.
Use `Nothing` for `E` when a message can't fail, and `Unit` for `A` when there is nothing to reply (`Nothing` is not allowed for `A`).
A message can also reply with a stream, see [Streaming](#streaming).

```scala
enum GuildMessage[+E, +A] {
  case Join(userId: String)  extends GuildMessage[GuildFull, Set[String]]
  case Leave(userId: String) extends GuildMessage[Nothing, Unit]
  case Members()             extends GuildMessage[Nothing, Set[String]] with Concurrent
}
```

Messages can extend marker traits that change how they are processed:
- `Concurrent`: the message runs alongside the other messages, ignoring the `concurrency` option (useful for read-only messages).
- `Uninterruptible`: the message is not interrupted when its sender stops waiting for it (e.g. after a timeout).

## Handlers

The behavior given to `Sharding.registerEntity` is called with the entity ID when an entity starts, and returns the **handler** that processes its messages.
Its `handle` method takes a message and returns its reply, and pattern matching on the message refines `E` and `A` to the types declared by each message.
Entity and topic types must all have different names.

- The `Scope` given to the behavior is closed when the entity stops, after its last message was processed: use it to acquire resources or save the entity state.
- If the behavior fails, the messages sent to the entity die with that error.
- If processing a message dies with a defect, the entity is stopped and the next message starts a new one.
- `Sharding.terminateLocalEntity` stops an entity explicitly (typically from the entity itself), after completing the messages it already received. The next message starts it again.

## Entity options

`registerEntity` takes an optional `EntityOptions` to configure each entity type:
- `maxIdleTime`: time without processing any message after which an entity is stopped (defaults to `Config.entityMaxIdleTime`). An entity with messages in progress or open streams is never stopped.
- `concurrency`: maximum number of messages processed at the same time by an entity. The default is 1: messages are processed one at a time, in order.
- `mailboxCapacity`: maximum number of messages waiting or being processed by an entity (4096 by default, `EntityOptions.Unbounded` for no limit). Beyond that, messages fail with `MailboxFull`.
- `loadEntity`: whether a message should start the entity when it isn't running. If not, the message is discarded: `sendDiscard` succeeds and `send` fails with `MessageDiscarded`.

```scala
Sharding.registerEntity(Guild, EntityOptions(maxIdleTime = Some(5.minutes), concurrency = 4))(behavior)
```

## Sending messages

A `Messenger` offers several ways to send messages, each taking the ID of the target entity:
- `send` sends a message and waits for its reply.
- `sendDiscard` sends a message and returns once it is in the entity's mailbox, without waiting for it to be processed.
- `stream` sends a message whose reply is a stream (see [Streaming](#streaming)).
- `duplex` sends a message along with a stream of inputs, and receives a stream of replies.
- `sendStream` sends a stream of messages that reply with nothing (`Msg[Nothing, Unit]`), in order, and returns once they are all in the entity's mailbox. It stops at the first rejected message (e.g. `MailboxFull`).
- `streamAutoRestart` and `duplexAutoRestart` are like `stream` and `duplex`, but restart the stream when the entity is rebalanced.
  The message is built from a cursor, which is updated with each reply so that the stream restarts where it ended.

The compiler rejects messages whose reply is a stream in `send` and `sendDiscard`.

```scala
for {
  guild   <- Sharding.messenger(Guild)
  _       <- guild.sendDiscard("guild1")(Leave("user1"))
  members <- guild.send("guild1")(Members())
} yield members
```

`Sharding.messenger` takes an optional `MessengerTimeout`: `InheritConfigTimeout` (the default) uses `Config.sendTimeout`, `Timeout(duration)` sets a specific timeout, and `NoTimeout` waits forever.
The timeout only applies to `send` and `sendDiscard`.
When it is reached, the sender receives `SendTimeout`, and for `send` the processing of the message is interrupted (unless it is `Uninterruptible`).

## Errors

Besides the error type declared by the message, calls can fail with a `ClusterError`:
- `MailboxFull`: the mailbox of the entity is full, the message was not processed.
- `SendTimeout`: `send` or `broadcast` got no reply, or `sendDiscard` got no acknowledgment, within the timeout.
- `PodUnavailable`: a pod is not responsive (only returned by `broadcast`, since messages to entities are retried).
- `StreamCancelled`: a stream of replies was interrupted after it started (e.g. the entity was rebalanced).
- `InvalidShardId`: the shard ID computed for an entity is invalid.
- `EntityInterrupted`: the entity was stopped before the message was fully processed (e.g. it exceeded `entityTerminationTimeout`). The message may have been partially processed.
- `MessageDiscarded`: the entity was not running and `loadEntity` rejected the message.

A defect in a remote entity is received as a defect (`ZIO.die`) containing a `RemoteDefect`.

## Delivery guarantees

- If the pod hosting an entity becomes unavailable while processing a message, the message is sent again once the entity has moved, so it may be processed more than once.
- A message is not sent again once `sendDiscard` returned, or when it fails with `EntityInterrupted`.
- A message whose reply is a stream is sent again only until the first element was received. After that, the stream fails with `StreamCancelled` if the entity is rebalanced (use `streamAutoRestart` or `duplexAutoRestart` to restart it).
- When a `duplex` or `sendStream` is sent again, its input stream is run again from the start, so it must be one that can be run more than once.

## Streaming

A message can reply with a stream by declaring a `Streamed[E2, A]` reply type: the entity can fail with `E` before the stream starts, then the stream emits values of type `A` and can fail with `E2`.
```scala
enum GuildMessage[+E, +A] {
  // ...
  case MemberStream() extends GuildMessage[Nothing, Streamed[Nothing, String]]
}

// in the handler
case GuildMessage.MemberStream() => state.get.map(members => Streamed(ZStream.fromIterable(members)))

// on the sender side
guild.stream("guild1")(MemberStream()).runCollect
```
Other messages are processed while the stream is running.

Similarly, a message with a `Duplex[In, E2, A]` reply type receives a stream of inputs of type `In` from the sender: the handler returns a function from the stream of inputs to the stream of replies. Use `duplex` to send it.
```scala
case DepositAll() extends AccountMessage[Nothing, Duplex[Int, Nothing, Int]]

// in the handler
case DepositAll() =>
  ZIO.succeed(Duplex[Int, Nothing, Int](amounts => amounts.mapZIO(amount => balance.updateAndGet(_ + amount))))

// on the sender side
account.duplex("account1")(DepositAll(), ZStream(1, 2, 3)).runCollect
```

## Topics

A topic type works like an entity type, except that its messages are broadcast to all pods: each pod runs one instance per topic name.
It is registered with `Sharding.registerTopic`, which takes the same options as entities.
To send messages, use a `Broadcaster`, which we get by calling `Sharding.broadcaster`:
- `broadcastDiscard` sends a message to all pods without waiting for it to be processed.
- `broadcast` sends a message to all pods and waits for their replies. It returns the result of each pod, as a `Map[PodAddress, Exit[E | ClusterError, A]]`.

```scala
object Notifications extends TopicType[NotificationMessage]("notifications")

for {
  _             <- Sharding.registerTopic(Notifications)(behavior)
  notifications <- Sharding.broadcaster(Notifications)
  _             <- notifications.broadcastDiscard("news")(Notify("Hello"))
} yield ()
```
