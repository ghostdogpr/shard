package shard.interfaces

import shard.interfaces.MessageCodec.ReplyCodec

/**
 * Per-message-type codec used by Shard to serialise entity / topic messages and
 * their replies on the wire.
 *
 * Carried by `RecipientType[Msg]` via an implicit `MessageCodec[Msg]` parameter, so each
 * `EntityType` / `TopicType` declaration picks up the codec from the surrounding scope.
 *
 * Implementations decide how `Msg` is encoded and — for reply-bearing variants — which
 * encoder/decoder pair to use for the reply type.
 * For uniform backends (e.g. Kryo) the reply codec is the same regardless of which
 * variant the message is; for variant-aware backends (e.g. Proteus) it depends on the
 * concrete variant.
 */
trait MessageCodec[Msg[+_, +_]] {

  /**
   * Encode a message to bytes for transport.
   */
  def encodeMessage(message: Msg[Any, Any]): Array[Byte]

  /**
   * Decode a message from bytes.
   */
  def decodeMessage(bytes: Array[Byte]): Msg[Any, Any]

  /**
   * Get the codecs used to encode and decode the replies and errors of a message.
   */
  def replyCodec(message: Msg[Any, Any]): ReplyCodec
}

object MessageCodec {
  type Encoder    = Any => Array[Byte]
  type Decoder[A] = Array[Byte] => A

  /**
   * Create a codec that uses the same `Codec` for messages and all their replies.
   */
  def uniform[Msg[+_, +_]](codec: Codec): MessageCodec[Msg] =
    new MessageCodec[Msg] {
      private val replies: ReplyCodec = ReplyCodec(codec, codec, codec, codec)

      def encodeMessage(message: Msg[Any, Any]): Array[Byte] = codec.encode(message)
      def decodeMessage(bytes: Array[Byte]): Msg[Any, Any]   = codec.decode(bytes).asInstanceOf[Msg[Any, Any]]
      def replyCodec(message: Msg[Any, Any]): ReplyCodec     = replies
    }

  final case class Codec(encode: Encoder, decode: Decoder[Any])

  object Codec {

    /**
     * A codec for `Nothing`, which is never called.
     */
    val nothing: Codec =
      Codec(
        value => throw new IllegalStateException(s"Cannot encode $value: the expected type is Nothing"),
        _ => throw new IllegalStateException("Cannot decode a value: the expected type is Nothing")
      )

    /**
     * A codec for `Unit`, which needs no bytes.
     */
    val unit: Codec = Codec(_ => Array.emptyByteArray, _ => ())
  }

  /**
   * The codecs for the replies of a given message `Msg[E, A]`.
   *
   * @param error codec for `E`
   * @param value codec for `A`, or for the elements of the stream when `A` is `Streamed[E2, B]` or `Duplex[In, E2, B]`
   * @param streamError codec for `E2` when `A` is `Streamed[E2, B]` or `Duplex[In, E2, B]`
   * @param streamInput codec for `In` when `A` is `Duplex[In, E2, B]`
   */
  final case class ReplyCodec(error: Codec, value: Codec, streamError: Codec, streamInput: Codec)
}
