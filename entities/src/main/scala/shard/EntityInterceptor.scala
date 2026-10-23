package shard

import zio.IO

/**
 * Wraps the entities of every entity or topic type and the processing of their messages, e.g. to add log annotations,
 * timing or tracing. Set it on `Config`. It wraps the `MessageInterceptor` of each type.
 */
trait EntityInterceptor { self =>

  /**
   * Wraps the whole life of one entity: its behavior, every message, and the finalizers of its scope.
   * The streams of `Streamed` and `Duplex` replies run outside of it.
   * It must run `effect` once. If it dies, the messages sent to the entity fail with `EntityDefect`.
   */
  def aroundEntity[E, A](recipientType: String, entityId: String)(effect: IO[E, A]): IO[E, A] = effect

  /**
   * Wraps the processing of one message by the handler, which completes with the message's reply.
   * For `Streamed` and `Duplex` replies, it wraps the creation of the stream, not the stream itself.
   * Its failures and defects are those of the message.
   */
  def aroundMessage[E, A](recipientType: String, entityId: String, message: Any)(effect: IO[E, A]): IO[E, A] = effect

  /**
   * Combines two interceptors, `this` one wrapping `that` one.
   */
  final def ++(that: EntityInterceptor): EntityInterceptor =
    new EntityInterceptor {
      override def aroundEntity[E, A](recipientType: String, entityId: String)(effect: IO[E, A]): IO[E, A] =
        self.aroundEntity(recipientType, entityId)(that.aroundEntity(recipientType, entityId)(effect))

      override def aroundMessage[E, A](recipientType: String, entityId: String, message: Any)(
        effect: IO[E, A]
      ): IO[E, A] =
        self.aroundMessage(recipientType, entityId, message)(
          that.aroundMessage(recipientType, entityId, message)(effect)
        )
    }
}

object EntityInterceptor {

  /**
   * An interceptor that does nothing.
   */
  val none: EntityInterceptor = new EntityInterceptor {}
}
