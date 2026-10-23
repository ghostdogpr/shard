package shard

import zio.IO

/**
 * Wraps the entities of a type and the processing of their messages, e.g. to add log annotations, timing or tracing.
 * Set it on `EntityOptions` for one type, or on `Config` for every type (an `EntityInterceptor[AnyMessage]`, which
 * wraps the one of each type).
 *
 * Pattern matching on `message` refines `E` and `A` to the types declared by each variant, as in
 * `EntityHandler.handle`:
 * {{{
 *   new EntityInterceptor[GuildMessage] {
 *     override def aroundMessage[E, A](recipientType: RecipientType[?], entityId: String, message: GuildMessage[E, A])(
 *       effect: IO[E, A]
 *     ): IO[E, A] =
 *       message match {
 *         case GuildMessage.Join(userId) => effect.tap(members => ZIO.logInfo(s"$userId joined: $members"))
 *         case _                         => effect
 *       }
 *   }
 * }}}
 */
trait EntityInterceptor[-Msg[_, _]] { self =>

  /**
   * Wraps the whole life of one entity: its behavior, every message, and the finalizers of its scope.
   * The streams of `Streamed` and `Duplex` replies run outside of it.
   * It must run `effect` once. If it dies, the messages sent to the entity fail with `EntityDefect`.
   */
  def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(effect: IO[E, A]): IO[E, A] = effect

  /**
   * Wraps the processing of one message by the handler, which completes with the message's reply.
   * For `Streamed` and `Duplex` replies, it wraps the creation of the stream, not the stream itself.
   * Its failures and defects are those of the message.
   */
  def aroundMessage[E, A](recipientType: RecipientType[?], entityId: String, message: Msg[E, A])(
    effect: IO[E, A]
  ): IO[E, A] =
    effect

  /**
   * Combines two interceptors, `this` one wrapping `that` one.
   */
  final def ++[M[E, A] <: Msg[E, A]](that: EntityInterceptor[M]): EntityInterceptor[M] =
    if (self eq EntityInterceptor.none) that
    else if (that eq EntityInterceptor.none) self
    else
      new EntityInterceptor[M] {
        override def aroundEntity[E, A](recipientType: RecipientType[?], entityId: String)(effect: IO[E, A]): IO[E, A] =
          self.aroundEntity(recipientType, entityId)(that.aroundEntity(recipientType, entityId)(effect))

        override def aroundMessage[E, A](recipientType: RecipientType[?], entityId: String, message: M[E, A])(
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
  val none: EntityInterceptor[AnyMessage] = new EntityInterceptor[AnyMessage] {}
}

/**
 * The messages of any type, for an `EntityInterceptor` that applies to every type.
 */
type AnyMessage[E, A] = Any
