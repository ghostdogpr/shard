# Delivery guarantees

Each message has a delivery guarantee, which decides whether it is sent again when its pod fails: best effort by default, or at most once for messages that extend the `AtMostOnce` [marker trait](entities.md#messages).

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
