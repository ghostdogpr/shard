package shard

import shard.errors.ClusterError
import zio.{ Exit, UIO }

/**
 * An interface to communicate with a remote broadcast receiver
 * @tparam Msg the type of message that can be sent to this broadcast receiver
 */
trait Broadcaster[Msg[_, _]] {

  /**
   * Broadcast a message without waiting for a response (fire and forget)
   */
  def broadcastDiscard[E, A](topic: String)(message: Msg[E, A])(using Unary[A]): UIO[Unit]

  /**
   * Broadcast a message and wait for a response from each consumer
   */
  def broadcast[E, A](topic: String)(message: Msg[E, A])(using
    Unary[A]
  ): UIO[Map[PodAddress, Exit[E | ClusterError, A]]]
}
