package shard

import zio.stream.ZStream

/**
 * The reply type of a message that replies with a stream of `A` that can fail with `E`.
 * Send it with `Messenger#stream`. Other messages are processed while the stream is running.
 */
final case class Streamed[+E, +A](stream: ZStream[Any, E, A])

/**
 * The reply type of a message that receives a stream of `In` and replies with a stream of `A` that can fail with `E`.
 * Send it with `Messenger#duplex`.
 */
final case class Duplex[In, +E, +A](run: ZStream[Any, Nothing, In] => ZStream[Any, E, A])
