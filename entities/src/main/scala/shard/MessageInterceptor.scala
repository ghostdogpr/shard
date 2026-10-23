package shard

import zio.IO

/**
 * Wraps the entities of one entity or topic type and the processing of their messages, e.g. to record the replies of
 * some messages. Set it on `EntityOptions`. The interceptor of `Config` wraps it.
 *
 * Pattern matching on `message` refines `E` and `A` to the types declared by each variant, as in
 * `EntityHandler.handle`:
 * {{{
 *   new MessageInterceptor[GuildMessage] {
 *     override def aroundMessage[E, A](recipientType: String, entityId: String, message: GuildMessage[E, A])(
 *       effect: IO[E, A]
 *     ): IO[E, A] =
 *       message match {
 *         case GuildMessage.Join(userId) => effect.tap(members => ZIO.logInfo(s"$userId joined: $members"))
 *         case _                         => effect
 *       }
 *   }
 * }}}
 */
trait MessageInterceptor[Msg[_, _]] {

  /**
   * Wraps the whole life of one entity, like `EntityInterceptor.aroundEntity`.
   */
  def aroundEntity[E, A](recipientType: String, entityId: String)(effect: IO[E, A]): IO[E, A] = effect

  /**
   * Wraps the processing of one message by the handler, like `EntityInterceptor.aroundMessage`.
   */
  def aroundMessage[E, A](recipientType: String, entityId: String, message: Msg[E, A])(effect: IO[E, A]): IO[E, A] =
    effect
}

object MessageInterceptor {

  /**
   * An interceptor that does nothing.
   */
  def none[Msg[_, _]]: MessageInterceptor[Msg] = new MessageInterceptor[Msg] {}
}
