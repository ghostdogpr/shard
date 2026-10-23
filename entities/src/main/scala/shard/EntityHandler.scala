package shard

import zio.IO

/**
 * The logic of an entity or topic: processes each message and returns its reply.
 * Pattern matching on the message refines `E` and `A` to the types declared by each variant:
 * {{{
 *   new Guild.Handler {
 *     def handle[E, A](message: GuildMessage[E, A]): IO[E, A] =
 *       message match {
 *         case GuildMessage.Join(userId)  => join(userId)
 *         case GuildMessage.Leave(userId) => leave(userId)
 *       }
 *   }
 * }}}
 */
trait EntityHandler[Msg[_, _]] {

  /**
   * Process a message and return its reply.
   */
  def handle[E, A](message: Msg[E, A]): IO[E, A]
}
