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
- During a rolling update that marks a message `AtMostOnce`, a sender on the previous version can still send the message again, and for `send`, a receiver on the previous version can still interrupt it when its sender times out.
