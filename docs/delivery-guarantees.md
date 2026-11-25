# Delivery guarantees

Each message has a delivery guarantee, which decides whether it is sent again when its pod fails: best effort by default, at most once for messages that extend the `AtMostOnce` [marker trait](entities.md#messages), or at least once for those that extend `AtLeastOnce`.

## Best effort (default)

- If the pod hosting an entity becomes unavailable while processing a message, the message is sent again once the entity has moved, so it may be processed more than once.
- A message is not sent again once `sendDiscard` returned, or when it fails with `EntityInterrupted`.
- A message whose reply is a stream is sent again only until the first element was received, or for a `duplex` until its inputs started being read. After that, the stream fails with `StreamCancelled` if the entity is rebalanced or its pod becomes unavailable (use `streamAutoRestart` or `duplexAutoRestart` to restart it).
- When a `sendStream` is sent again, its stream of messages is run again from the start, so it must be one that can be run more than once.

## At most once (`AtMostOnce`)

- The message is sent again only if it certainly wasn't processed: it never left the sender (e.g. the connection was refused), or its pod answered that it doesn't host the entity.
- If its pod becomes unavailable after it was sent, the call fails with `DeliveryUnknown`.
- Each call is a new attempt: calling again after a `SendTimeout` or a `DeliveryUnknown` may process the message again.
- For a message whose reply is a stream, the handler that returns the stream runs at most once. Until the first element is received, or for a `duplex` until its inputs start being read, the rules above apply, except that the stream fails with `StreamCancelled` if its entity stops. After that, it behaves as with best effort. `streamAutoRestart` and `duplexAutoRestart` restart it on `StreamCancelled`, as a new call, but not on `DeliveryUnknown`.
- A `sendStream` is sent again from the start only while none of its `AtMostOnce` messages was sent. After that, a failure that would send it again makes it fail with `DeliveryUnknown`.
- An `AtMostOnce` message sent to a topic is processed at most once on each pod. With `broadcast`, the result for a pod where it may or may not have been processed is `DeliveryUnknown`.
- During a rolling update that marks a message `AtMostOnce`, the guarantee may not hold while some pods still run the previous version: the message can be processed more than once, or interrupted when its sender times out.

## At least once (`AtLeastOnce`)

An `AtLeastOnce` message is identified by its **delivery key**, a `String` field of the message given to the marker. A message with no natural key can use an ID that the caller generates when building it.

```scala
enum AccountMessage[E, A] {
  case Deposit(txId: String, amount: Long) extends AccountMessage[Nothing, Long] with AtLeastOnce(txId)
  case Withdraw(txId: String, amount: Long) extends AccountMessage[NotEnoughFunds, Long] with AtLeastOnce(txId)
}
```

The pod hosting the entity stores the message in [message storage](customization.md#message-storage), for example [on Redis](customization.md#redis) or [on Postgres](customization.md#postgres), before running it, then stores its first result, a reply or a typed error. Every `send` with the same key gets that result without running the message again, and a `send` with a key whose message is running waits for its result.

- A key belongs to an entity: messages of any case sent to the same entity with the same key are one message, whatever their other fields. The stored result is decoded with the codecs of the case that is sent, so cases sharing a key need compatible reply and error types. Reusing a key for a different message is not supported.
- A result is kept for the `retention` of the entity type (24 hours by default). After that, the key is free, and a `send` with it is a new message.
- Once stored, the message runs and finishes even if its sender gets `SendTimeout`.
- `sendDiscard` returns once the message is in the entity's mailbox, or at once if its key has a result. If the entity rejects a new message before that, for example with `MailboxFull`, the message isn't stored, so it can be sent again.
- `loadEntity` only applies to new keys: a new message that it discards isn't stored, and a message whose key is already stored is never discarded.
- Each message of a `sendStream` is stored and queued like a `sendDiscard`, one at a time, so the latency of message storage adds up over a long stream. When the `sendStream` is sent again from the start, its messages whose key has a result or is running don't run again, and one whose key is a dead letter fails it with `DeadLettered`.
- Calls that return a stream of replies (`stream`, `duplex` and their auto-restart variants) and topics don't use the guarantee: sent with them, the message is best effort. A message can't have both `AtMostOnce` and `AtLeastOnce`.

### Recovery

- The pod that owns the shard of an entity runs the stored messages that have no result and aren't running, when the entity type is registered and then every `rescanInterval` (10 seconds by default). So a message whose pod died, or was rebalanced before the message finished, runs on the next owner of its shard at its next scan, or sooner if it is sent again. Each scan sets the `shard.pending_messages` [metric](metrics.md) to the number of stored messages without a result in the shards of the pod, including those that are running.
- The owner starts the stored messages of an entity in the order they were stored, apart from `Concurrent` messages, which run alongside the others. A new message sent to the entity meanwhile can run before them. A message that dies can run again after the messages queued behind it, which are queued again on a new entity in any order. A sender that needs its messages to run in strict order should wait for the outcome of each one before sending the next.
- A rebalance or a shutdown lets running and queued messages finish for up to `entityTerminationTimeout`, as for other messages. A message that doesn't finish in time, or that fails with `EntityInterrupted`, stores no result, so it runs again.
- A stored message that runs again is added to the mailbox even when it is full. It still counts toward `mailboxCapacity` for new messages.
- If its entity fails to start, the message fails with `EntityDefect` and stays stored, and it runs once the entity starts, without using an attempt.
- If message storage can't be reached, the message fails with `StorageUnavailable`, and Shard doesn't send it again. If the message was stored anyway, or its result couldn't be stored, its owner runs it again.

### Retries

- A message that dies with a defect runs again on the `retrySchedule` of the entity type, by default 5 attempts, 0.2, 0.4, 0.8 and 1.6 seconds apart. Meanwhile, the sends of its key wait for its outcome, up to their timeout. As for other messages, the defect stops the entity, so the next messages run on a new one, and the message is queued again after them once its delay is over.
- A message that its entity stops before it runs, for example because a message before it died, is queued again without using an attempt.
- Attempts are counted by the pod that runs the message and start over on the next owner of its shard. So an attempt cut short by a rebalance or a shutdown doesn't count, and a message that brings its pod down every time is retried on each new owner.
- A message becomes a dead letter when it runs out of attempts, when it is stored but can't be decoded, or when its reply or typed error can't be encoded. A dead letter never expires, and every send of its key fails with `DeadLettered` (`EntityDefect` from a pod of an earlier version). Each new dead letter is logged as a warning and counted by the `shard.dead_letters` [metric](metrics.md).

### Dead letters

`DeadLetters.list(entityType)` streams the dead letters of an entity type, each with its entity ID, delivery key and reason. Any pod can retry or drop a dead letter, and both tools return `false` if its key has none:

- `DeadLetters.retry(entityType, entityId, deliveryKey)` stores its message again, so that the owner of its shard runs it at its next scan, after the other stored messages of its entity, or sooner if its key is sent again. It gets the whole `retrySchedule` again, and its result becomes the result of its key. If message storage fails during the retry, the dead letter may be removed without its message being stored again, which is logged as an error.
- `DeadLetters.drop(entityType, entityId, deliveryKey)` removes the dead letter, so the next send with its key is a new message.

### Upgrades

- A pod of an earlier version runs the message as best effort, so during the rolling update that adds the marker, the message may run more than once.
- Stored messages and results are encoded with the codec of the entity type, so the later versions of the app must still be able to decode them, which is easier with a codec whose format can evolve, such as [Proteus](customization.md#proteus).
