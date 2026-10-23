package shard.proteus

import _root_.proteus.{ Dependency, ProtobufCodec, ProtobufDeriver }
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
      collectVariantCodecs[Msg, m.MirroredElemTypes].map(_.replyCodec).toArray
    )

  /**
   * Create a Proteus `Dependency` containing the message type and the error, reply and stream types of its variants.
   * Render it to get a .proto file describing the message contract.
   */
  inline def dependency[Msg[+_, +_]](name: String, packageName: String)(using
    m: Mirror.SumOf[Msg[Any, Any]],
    deriver: ProtobufDeriver
  ): Dependency =
    collectVariantCodecs[Msg, m.MirroredElemTypes]
      .flatMap(_.protobufCodecs)
      .foldLeft(
        Dependency(name, packageName).add(using summonOrDeriveProtobufCodec[Msg[Any, Any]])
      )((dependency, codec) => dependency.add(using codec))

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

  // The codecs of an error, reply or stream type: `codec` for the reply codec, `protobufCodec` for the dependency.
  private final case class TypeCodec(codec: Codec, protobufCodec: Option[ProtobufCodec[?]])

  private object TypeCodec {
    val nothing: TypeCodec = TypeCodec(Codec.nothing, None)
  }

  // The codecs of a variant's error, reply, stream error and stream input types.
  private final case class VariantCodecs(
    error: TypeCodec,
    value: TypeCodec,
    streamError: TypeCodec,
    streamInput: TypeCodec
  ) {
    def replyCodec: ReplyCodec =
      ReplyCodec(error.codec, value.codec, streamError.codec, streamInput.codec)

    def protobufCodecs: List[ProtobufCodec[?]] =
      List(error, value, streamError, streamInput).flatMap(_.protobufCodec)
  }

  private inline def collectVariantCodecs[Msg[+_, +_], Variants <: Tuple](using
    deriver: ProtobufDeriver
  ): List[VariantCodecs] =
    inline erasedValue[Variants] match {
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => variantCodecsOf[Msg, head] :: collectVariantCodecs[Msg, tail]
    }

  private inline def variantCodecsOf[Msg[+_, +_], Variant](using deriver: ProtobufDeriver): VariantCodecs =
    inline erasedValue[Variant] match {
      case _: Msg[e, Streamed[e2, a]]   =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], typeCodecOf[e2], TypeCodec.nothing)
      case _: Msg[e, Duplex[in, e2, a]] =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], typeCodecOf[e2], typeCodecOf[in])
      case _: Msg[e, a]                 =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], TypeCodec.nothing, TypeCodec.nothing)
      case _                            =>
        error("Cannot derive Proteus codecs for the message type: a variant doesn't extend the message type")
    }

  /**
   * Get the codecs of a reply or error type.
   */
  private inline def typeCodecOf[A](using deriver: ProtobufDeriver): TypeCodec =
    summonFrom {
      case _: (A =:= Unit) => TypeCodec(Codec.unit, None)
      case _               =>
        val protobufCodec = protobufCodecOf[A]
        TypeCodec(protobufCodec.fold(Codec.nothing)(fromProtobufCodec), protobufCodec)
    }

  /**
   * Get the `ProtobufCodec` of a reply or error type, or `None` for `Unit` and `Nothing`.
   * Uses a `ProtobufCodec` in scope if there is one, otherwise derives one.
   */
  private inline def protobufCodecOf[A](using deriver: ProtobufDeriver): Option[ProtobufCodec[A]] =
    summonFrom {
      case _: (A =:= Nothing)      => None
      case _: (A =:= Unit)         => None
      case _: (A =:= Any)          =>
        error(
          "Cannot derive Proteus codecs for the message type: a variant has no fixed error or reply type. " +
            "Each variant must extend the message type with fixed type arguments (e.g. `extends Msg[MyError, MyReply]`): " +
            "variants can't have type parameters, and intermediate sealed traits must fix both type arguments."
        )
      case codec: ProtobufCodec[A] => Some(codec)
      case _: Mirror.Of[A]         => Some(ProtobufCodec.derived[A](using deriver))
      case _                       =>
        error(
          "Cannot derive Proteus codecs for the message type: a reply or error type has no Mirror or in-scope ProtobufCodec. " +
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
