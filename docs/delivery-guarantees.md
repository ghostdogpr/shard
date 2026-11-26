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

- The pod hosting the entity stores the message in [message storage](customization.md#message-storage), for example [on Redis](customization.md#redis), before running it. Its first result, a reply or a typed error, is stored, and every `send` with the same key gets that result without running the message again.
- A `send` with a key whose message is running waits for its result, so a message sent again after its connection dropped runs once.
- The pod that owns the shard of an entity runs the stored messages of the entity that have no result, without waiting for them to be sent again. It looks for them when the entity type is registered, and then every `rescanInterval`, skipping those that are running. So a message whose pod died before its result was stored runs again on the next owner of its shard. The scan at registration and each periodic scan set the `shard.pending_messages` [metric](metrics.md) to the number of these messages in all the shards of the pod.
- The owner starts the stored messages that it finds for an entity in the order they were stored, apart from `Concurrent` messages, which run alongside the others. A new message sent to the entity meanwhile can run before them. A message that dies with a defect can run again after the messages queued behind it, which are queued again on a new entity in any order. A sender that needs its messages to run in strict order should send each one and wait for its outcome before sending the next.
- A rebalance or a shutdown lets running and queued messages finish, as for other messages, for up to `entityTerminationTimeout`. A message that doesn't finish in time stores no result, so it runs again on the next owner of its shard.
- If its result can't be stored, the message fails with `StorageUnavailable` and stays stored, so its owner runs it again on its next scan, or when it is sent again.
- A stored message that runs again, started by its owner or by a `send` of its key, is added to the mailbox even when it is full. It still counts toward `mailboxCapacity` for new messages.
- If its entity fails to start, the message fails with `EntityDefect` (or `EntityInterrupted` if the start was interrupted) and stays stored, and its owner runs it once the entity starts. This doesn't use an attempt.
- A key belongs to an entity: messages of any case sent to the same entity with the same key are one message, whatever their other fields. The stored result is decoded with the codecs of the case that is sent, so cases sharing a key need compatible reply and error types. Reusing a key for a different message is not supported.
- A reply or a typed error is kept for the `retention` of the entity type (24 hours by default). After that, the key is free, and a `send` with it is a new message.
- A message that dies with a defect is run again on the `retrySchedule` of the entity type, by default 5 attempts, 0.2, 0.4, 0.8 and 1.6 seconds apart. Meanwhile, the sends of its key wait for its outcome, up to their `sendTimeout`. The defect stops the entity as for other messages, so the next messages to the entity run on a new one, and the message is queued again after them once its delay is over.
- Attempts are counted by the pod that runs the message, and don't carry over to the next owner of its shard, which starts again with the whole schedule. So an attempt cut short by a rebalance or a shutdown doesn't count, and a message that brings its pod down every time keeps being retried on each new owner.
- A message becomes a dead letter when it runs out of attempts, when it is stored but can't be decoded, or when its reply or typed error can't be encoded. A dead letter is kept with no expiry, and its waiting senders and every later `send` or `sendDiscard` of its key fail with `DeadLettered`. Senders on a pod of an earlier version get `EntityDefect` instead. Each new dead letter is logged as a warning with its entity type, entity ID, delivery key and reason, and counted by the `shard.dead_letters` [metric](metrics.md). See [Dead letters](#dead-letters) to retry or drop one.
- A message that its entity stops before it runs, for example because a message before it died, is queued again on a new entity while its pod owns the shard, without using an attempt. Another interruption (`EntityInterrupted`) stores no result, so the message runs again on the next scan of its owner, or when it is sent again.
- If message storage can't be reached when the message is stored, the message fails with `StorageUnavailable` at once, and Shard doesn't send it again. If it was stored anyway, its owner still runs it.
- Its sender never interrupts it: once stored, the message runs and finishes even if its sender gets `SendTimeout`.
- `sendDiscard` works like a `send` that doesn't wait for the result: it returns once the message is in the entity's mailbox, and the message then runs in the background. If the key has a result, it returns at once, and if the key is running, it returns once that message is in the mailbox. If the entity rejects a new message before it is in the mailbox, for example with `MailboxFull`, the message isn't stored, so it can be sent again.
- `loadEntity` only applies to new keys: a new message that it discards isn't stored, and as for other messages, `sendDiscard` succeeds and `send` fails with `MessageDiscarded`. `loadEntity` never discards a message with a key that is already stored.
- Each message of a `sendStream` is stored and queued like a `sendDiscard`, so when a `sendStream` is sent again from the start after a failure, its messages whose key has a result or is running don't run again, and a message whose key is a dead letter fails the `sendStream` with `DeadLettered` each time. The messages are stored and queued one at a time, so the latency of message storage adds up over a long stream. If message storage can't be reached, the `sendStream` fails with `StorageUnavailable` and isn't sent again.
- Calls that return a stream of replies (`stream`, `duplex` and their auto-restart variants) and topics don't use it: sent with them, the message is best effort. A message can't have both `AtMostOnce` and `AtLeastOnce`.
- A pod of an earlier version runs it as best effort, so during the rolling update that adds the marker, the message may run more than once.
- Stored messages and results are encoded with the codec of the entity type, so the later versions of the app must still be able to decode them, which is easier with a codec whose format can evolve, such as [Proteus](customization.md#proteus).

### Dead letters

`DeadLetters.list(entityType)` streams the dead letters of an entity type from message storage, each with its entity ID, delivery key and reason, without decoding their messages. Any pod can retry or drop a dead letter, and both tools return `false` if its key has no dead letter:

- `DeadLetters.retry(entityType, entityId, deliveryKey)` stores its message again, so that the owner of its shard runs it at its next scan, after the other stored messages of its entity, or sooner if its key is sent again. It gets the whole `retrySchedule` again, and its result becomes the result of its key. If message storage fails during the retry, the dead letter may be removed without its message being stored again, which is logged as an error.
- `DeadLetters.drop(entityType, entityId, deliveryKey)` removes the dead letter, so its key is free at once and the next send with it is a new message.
