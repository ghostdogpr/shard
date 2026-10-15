package shard

import shard.interfaces.MessageCodec
import shard.proteus.given
import zio.Scope
import zio.test._

object ProteusMessageCodecSpec extends ZIOSpecDefault {

  final case class Reply(text: String, ok: Boolean)
  final case class StreamReply(value: Int)
  final case class Input(delta: Int)
  final case class Page[A](items: List[A])

  sealed trait Error
  object Error {
    final case class NotFound(id: Int) extends Error
    final case class Invalid()         extends Error
  }

  enum Msg[+E, +A] {
    case Plain(name: String, count: Int) extends Msg[Nothing, Unit]
    case WithReply(id: Int)              extends Msg[Error, Reply]
    case WithStreamReply(from: Int)      extends Msg[Error.Invalid, Streamed[Error, StreamReply]]
    case WithDuplex()                    extends Msg[Nothing, Duplex[Input, Nothing, StreamReply]]
    case Generic(replies: List[Reply])   extends Msg[Nothing, Page[Reply]]
    case Singleton                       extends Msg[Nothing, Reply]
  }

  private val codec = summon[MessageCodec[Msg]]

  private def roundtrip(msg: Msg[Any, Any]): Msg[Any, Any] =
    codec.decodeMessage(codec.encodeMessage(msg))

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ProteusMessageCodecSpec")(
      test("roundtrips messages") {
        val messages: List[Msg[Any, Any]] = List(
          Msg.Plain("hello", 42),
          Msg.WithReply(7),
          Msg.WithStreamReply(3),
          Msg.WithDuplex(),
          Msg.Generic(List(Reply("a", true), Reply("b", false))),
          Msg.Singleton
        )
        assertTrue(messages.map(roundtrip) == messages)
      },
      test("encodes Unit replies without bytes and has no error codec when the error type is Nothing") {
        val replyCodec = codec.replyCodec(Msg.Plain("hello", 42))
        val bytes      = replyCodec.value.encode(())
        val error      = scala.util.Try(replyCodec.error.encode(Error.Invalid()))
        assertTrue(bytes.isEmpty, replyCodec.value.decode(bytes) == (), error.isFailure)
      },
      test("roundtrips the reply and error of a variant") {
        val replyCodec = codec.replyCodec(roundtrip(Msg.WithReply(7)))
        val reply      = Reply("ok", true)
        val error      = Error.NotFound(7)
        assertTrue(
          replyCodec.value.decode(replyCodec.value.encode(reply)) == reply,
          replyCodec.error.decode(replyCodec.error.encode(error)) == error
        )
      },
      test("uses the element and stream error types of a Streamed reply") {
        val replyCodec = codec.replyCodec(Msg.WithStreamReply(3))
        val element    = StreamReply(99)
        val error      = Error.Invalid()
        val failure    = Error.NotFound(1)
        assertTrue(
          replyCodec.value.decode(replyCodec.value.encode(element)) == element,
          replyCodec.error.decode(replyCodec.error.encode(error)) == error,
          replyCodec.streamError.decode(replyCodec.streamError.encode(failure)) == failure
        )
      },
      test("uses the input type of a Duplex reply") {
        val replyCodec = codec.replyCodec(Msg.WithDuplex())
        val input      = Input(5)
        val element    = StreamReply(99)
        assertTrue(
          replyCodec.streamInput.decode(replyCodec.streamInput.encode(input)) == input,
          replyCodec.value.decode(replyCodec.value.encode(element)) == element
        )
      },
      test("supports generic reply types and singleton variants") {
        val replies = Page(List(Reply("a", true)))
        val generic = codec.replyCodec(Msg.Generic(Nil))
        val single  = codec.replyCodec(Msg.Singleton)
        assertTrue(
          generic.value.decode(generic.value.encode(replies)) == replies,
          single.value.decode(single.value.encode(Reply("s", true))) == Reply("s", true)
        )
      },
      test("rejects primitive reply types at compile time") {
        for {
          viaGiven  <- typeCheck {
                         """
                         enum Bad[+E, +A] {
                           case GetCount() extends Bad[Nothing, Int]
                         }
                         summon[shard.interfaces.MessageCodec[Bad]]
                         """
                       }
          // calling `derived` directly shows why
          viaDerive <- typeCheck {
                         """
                         enum Bad[+E, +A] {
                           case GetCount() extends Bad[Nothing, Int]
                         }
                         shard.proteus.ProteusMessageCodec.derived[Bad]
                         """
                       }
        } yield assertTrue(viaGiven.isLeft, viaDerive.left.exists(_.contains("wrap primitive types")))
      }
    )
}
