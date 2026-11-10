package shard.proteus

import _root_.proteus.{ Dependency, ProtobufCodec, ProtobufDeriver }
import shard.{ Duplex, Streamed }
import shard.interfaces.MessageCodec
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }

import scala.annotation.tailrec
import scala.compiletime.{ erasedValue, error, summonFrom }
import scala.compiletime.ops.int
import scala.deriving.Mirror

/**
 * An inline-derived `MessageCodec[Msg]` backed by Proteus.
 *
 * `Msg` must be a sealed trait or an enum whose variants extend it with fixed type arguments.
 * Error and reply types must be case classes, sealed traits, enums, `Unit`, `Nothing`, or have a `ProtobufCodec` in scope
 * that encodes a message or an enum.
 */
object ProteusMessageCodec {

  /**
   * The mirror of a message type, whose `MirroredMonoType` is `Msg[?, ?]`.
   */
  type MessageMirror[Msg[_, _]] =
    Mirror.Sum { type MirroredType[E, A] = Msg[E, A]; type MirroredElemTypes[E, A] <: Tuple }

  /**
   * Derive a `MessageCodec[Msg]` using Proteus.
   */
  inline def derived[Msg[_, _]](using m: MessageMirror[Msg], deriver: ProtobufDeriver): MessageCodec[Msg] =
    build[Msg](m, collectVariantCodecs[Msg, m.MirroredElemTypes[Any, Any]], messageCodec[Msg])

  /**
   * Create a Proteus `Dependency` containing the message type and the error, reply and stream types of its variants.
   * Render it to get a .proto file describing the message contract.
   */
  inline def dependency[Msg[_, _]](name: String, packageName: String)(using
    m: MessageMirror[Msg],
    deriver: ProtobufDeriver
  ): Dependency =
    collectVariantCodecs[Msg, m.MirroredElemTypes[Any, Any]]
      .flatMap(_.protobufCodecs)
      .foldLeft(
        Dependency(name, packageName).add(using messageCodec[Msg])
      )((dependency, codec) => dependency.add(using codec))

  private def build[Msg[_, _]](
    m: MessageMirror[Msg],
    variants: List[VariantCodecs],
    msgCodec: ProtobufCodec[Msg[Any, Any]]
  ): MessageCodec[Msg] = {
    requireRootCodec(msgCodec)
    val replyCodecs: Array[ReplyCodec] = variants.map(_.replyCodec).toArray
    new MessageCodec[Msg] {
      def encodeMessage[E, A](message: Msg[E, A]): Array[Byte] = msgCodec.encode(message.asInstanceOf[Msg[Any, Any]])
      def decodeMessage(bytes: Array[Byte]): Msg[Any, Any]     = msgCodec.decode(bytes)
      def replyCodec[E, A](message: Msg[E, A]): ReplyCodec     =
        replyCodecs(m.ordinal(message.asInstanceOf[m.MirroredMonoType]))
    }
  }

  /**
   * Get the `ProtobufCodec` of `Msg[?, ?]`, typed as `Msg[Any, Any]`.
   */
  private inline def messageCodec[Msg[_, _]](using
    m: MessageMirror[Msg],
    deriver: ProtobufDeriver
  ): ProtobufCodec[Msg[Any, Any]] =
    // `Msg[?, ?]` can't be written for an abstract `Msg`: `t` binds it once `Msg` is known
    inline erasedValue[Option[m.MirroredMonoType]] match {
      case _: Option[t] => summonOrDeriveProtobufCodec[t].asInstanceOf[ProtobufCodec[Msg[Any, Any]]]
    }

  // The codecs of an error, reply or stream type: `codec` for the reply codec, `protobufCodec` for the dependency.
  private final case class TypeCodec(codec: Codec, protobufCodec: Option[ProtobufCodec[?]])

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

  // Recurses on halves: one nested inline per variant would exceed the compiler's inline limit (`-Xmax-inlines`).
  private inline def collectVariantCodecs[Msg[_, _], Variants <: Tuple](using
    deriver: ProtobufDeriver
  ): List[VariantCodecs] =
    inline erasedValue[Variants] match {
      case _: (head *: EmptyTuple) => variantCodecsOf[Msg, head] :: Nil
      case _: (? *: ? *: ?)        =>
        concat(
          collectVariantCodecs[Msg, Tuple.Take[Variants, HalfSize[Variants]]],
          collectVariantCodecs[Msg, Tuple.Drop[Variants, HalfSize[Variants]]]
        )
    }

  private type HalfSize[Variants <: Tuple] = int./[Tuple.Size[Variants], 2]

  // By-name, so that each variant's inlined code gets its own JVM method: all of them in one would exceed 64 KB.
  private def concat(left: => List[VariantCodecs], right: => List[VariantCodecs]): List[VariantCodecs] =
    left ++ right

  private inline def variantCodecsOf[Msg[_, _], Variant](using deriver: ProtobufDeriver): VariantCodecs =
    inline erasedValue[Variant] match {
      case _: Msg[e, Streamed[e2, a]]   =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], typeCodecOf[e2], typeCodecOf[Nothing])
      case _: Msg[e, Duplex[in, e2, a]] =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], typeCodecOf[e2], typeCodecOf[in])
      case _: Msg[e, a]                 =>
        VariantCodecs(typeCodecOf[e], typeCodecOf[a], typeCodecOf[Nothing], typeCodecOf[Nothing])
      case _                            =>
        error("Cannot derive Proteus codecs for the message type: a variant doesn't extend the message type")
    }

  /**
   * Get the codecs of a reply or error type.
   * Uses a `ProtobufCodec` in scope if there is one, otherwise derives one.
   */
  private inline def typeCodecOf[A](using deriver: ProtobufDeriver): TypeCodec =
    summonFrom {
      case _: (A =:= Unit)                      => TypeCodec(Codec.unit, None)
      case _: (A =:= Nothing)                   => TypeCodec(Codec.nothing, None)
      case _: (A =:= Any)                       =>
        error(
          "Cannot derive Proteus codecs for the message type: a variant has no fixed error or reply type. " +
            "Each variant must extend the message type with fixed type arguments (e.g. `extends Msg[MyError, MyReply]`): " +
            "variants can't have type parameters, and intermediate sealed traits must fix both type arguments."
        )
      case codec: ProtobufCodec[A]              => fromProtobufCodec(codec)
      // `Option` and `List` have a `Mirror`, but Proteus can't encode them at the root
      case _: (A <:< (Option[Any] | List[Any])) => unsupportedRootType
      case _: Mirror.Of[A]                      => fromProtobufCodec(ProtobufCodec.derived[A](using deriver))
      case _                                    => unsupportedRootType
    }

  private inline def unsupportedRootType: Nothing =
    error(
      "Cannot derive Proteus codecs for the message type: a reply or error type can't be derived and has no in-scope ProtobufCodec. " +
        "Proteus requires a case class, sealed trait, or enum at the root — " +
        "wrap primitive types (Int, String, etc.) and collections (Option, List, etc.) in a case class."
    )

  private def fromProtobufCodec[A](codec: ProtobufCodec[A]): TypeCodec = {
    requireRootCodec(codec)
    TypeCodec(Codec(value => codec.encode(value.asInstanceOf[A]), bytes => codec.decode(bytes)), Some(codec))
  }

  // A codec given in scope is only known at runtime. Without this, it would fail on each message sent to another pod.
  @tailrec
  private def requireRootCodec(codec: ProtobufCodec[?]): Unit =
    codec match {
      case _: ProtobufCodec.Message[?] | _: ProtobufCodec.RecursiveMessage[?] | _: ProtobufCodec.Enum[?] => ()
      case transform: ProtobufCodec.Transform[?, ?]                                                      =>
        requireRootCodec(transform.codec)
      case _                                                                                             =>
        throw new IllegalArgumentException(
          s"Invalid Proteus codec: messages, replies and errors must be encoded as a message or an enum, but got $codec. " +
            "Wrap primitive types (Int, String, etc.) and collections (Option, List, etc.) in a case class."
        )
    }

  private inline def summonOrDeriveProtobufCodec[A](using deriver: ProtobufDeriver): ProtobufCodec[A] =
    summonFrom {
      case codec: ProtobufCodec[A] => codec
      case _                       => ProtobufCodec.derived[A](using deriver)
    }
}
