package shard.proteus

import _root_.proteus.{ ProtobufCodec, ProtobufDeriver }
import shard.{ Duplex, Streamed }
import shard.interfaces.MessageCodec
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }

import scala.compiletime.{ erasedValue, error, summonFrom }
import scala.deriving.Mirror

/**
 * An inline-derived `MessageCodec[Msg]` backed by Proteus.
 *
 * `Msg` must be a sealed trait or an enum whose variants extend it with fixed type arguments.
 * Error and reply types must be case classes, sealed traits, enums, `Unit`, `Nothing`, or have a `ProtobufCodec` in scope.
 */
object ProteusMessageCodec {

  /**
   * Derive a `MessageCodec[Msg]` using Proteus.
   */
  inline def derived[Msg[+_, +_]](using m: Mirror.SumOf[Msg[Any, Any]], deriver: ProtobufDeriver): MessageCodec[Msg] =
    build[Msg](
      summonOrDeriveProtobufCodec[Msg[Any, Any]],
      m.ordinal(_),
      collectReplyCodecs[Msg, m.MirroredElemTypes].toArray
    )

  private def build[Msg[+_, +_]](
    msgCodec: ProtobufCodec[Msg[Any, Any]],
    ordinal: Msg[Any, Any] => Int,
    replyCodecs: Array[ReplyCodec]
  ): MessageCodec[Msg] =
    new MessageCodec[Msg] {
      def encodeMessage(message: Msg[Any, Any]): Array[Byte] = msgCodec.encode(message)
      def decodeMessage(bytes: Array[Byte]): Msg[Any, Any]   = msgCodec.decode(bytes)
      def replyCodec(message: Msg[Any, Any]): ReplyCodec     = replyCodecs(ordinal(message))
    }

  private inline def collectReplyCodecs[Msg[+_, +_], Variants <: Tuple](using
    deriver: ProtobufDeriver
  ): List[ReplyCodec] =
    inline erasedValue[Variants] match {
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => replyCodecOf[Msg, head] :: collectReplyCodecs[Msg, tail]
    }

  private inline def replyCodecOf[Msg[+_, +_], Variant](using deriver: ProtobufDeriver): ReplyCodec =
    inline erasedValue[Variant] match {
      case _: Msg[e, Streamed[e2, a]]   => ReplyCodec(codecOf[e], codecOf[a], codecOf[e2], Codec.nothing)
      case _: Msg[e, Duplex[in, e2, a]] => ReplyCodec(codecOf[e], codecOf[a], codecOf[e2], codecOf[in])
      case _: Msg[e, a]                 => ReplyCodec(codecOf[e], codecOf[a], Codec.nothing, Codec.nothing)
      case _                            => error("Cannot derive a Proteus MessageCodec: a variant doesn't extend the message type")
    }

  /**
   * Get the codec of a reply or error type.
   * Uses a `ProtobufCodec` in scope if there is one, otherwise derives one.
   */
  private inline def codecOf[A](using deriver: ProtobufDeriver): Codec =
    summonFrom {
      case _: (A =:= Nothing)      => Codec.nothing
      case _: (A =:= Unit)         => Codec.unit
      case _: (A =:= Any)          =>
        error(
          "Cannot derive a Proteus MessageCodec: a variant has no fixed error or reply type. " +
            "Each variant must extend the message type with fixed type arguments (e.g. `extends Msg[MyError, MyReply]`): " +
            "variants can't have type parameters, and intermediate sealed traits must fix both type arguments."
        )
      case codec: ProtobufCodec[A] => fromProtobufCodec(codec)
      case _: Mirror.Of[A]         => fromProtobufCodec(ProtobufCodec.derived[A](using deriver))
      case _                       =>
        error(
          "Cannot derive a Proteus MessageCodec: a reply or error type has no Mirror or in-scope ProtobufCodec. " +
            "Proteus requires a case class, sealed trait, or enum at the root — " +
            "wrap primitive types (Int, String, etc.) in a case class."
        )
    }

  private def fromProtobufCodec[A](codec: ProtobufCodec[A]): Codec =
    Codec(value => codec.encode(value.asInstanceOf[A]), bytes => codec.decode(bytes))

  private inline def summonOrDeriveProtobufCodec[A](using deriver: ProtobufDeriver): ProtobufCodec[A] =
    summonFrom {
      case codec: ProtobufCodec[A] => codec
      case _                       => ProtobufCodec.derived[A](using deriver)
    }
}
