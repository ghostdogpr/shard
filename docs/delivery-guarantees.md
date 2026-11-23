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

- The pod hosting the entity stores the message in [message storage](customization.md#message-storage) before running it. Its first result, a reply or a typed error, is stored, and every `send` with the same key gets that result without running the message again.
- A `send` with a key whose message is running waits for its result, so a message sent again after its connection dropped runs once. If its pod dies before its result is stored, or its result can't be stored, the message runs again when it is sent again.
- A key belongs to an entity: messages of any case sent to the same entity with the same key are one message, whatever their other fields. The stored result is decoded with the codecs of the case that is sent, so cases sharing a key need compatible reply and error types. Reusing a key for a different message is not supported.
- A result is kept for the `retention` of the entity type (24 hours by default). After that, the key is free, and a `send` with it is a new message.
- A defect (`EntityDefect`), an interruption (`EntityInterrupted`) or a reply that fails to be encoded stores no result, so a `send` with the same key runs the message again.
- If message storage can't be reached, the message fails with `StorageUnavailable` at once, and Shard doesn't send it again.
- Its sender never interrupts it: once stored, the message runs and finishes even if its sender gets `SendTimeout`.
- Only `send` uses it. Sent any other way, such as with `sendDiscard`, `stream`, `sendStream` or to a topic, the message is best effort. A message can't have both `AtMostOnce` and `AtLeastOnce`.
- A pod of an earlier version runs it as best effort, so during the rolling update that adds the marker, the message may run more than once.
- Stored messages and results are encoded with the codec of the entity type, so the later versions of the app must still be able to decode them.
