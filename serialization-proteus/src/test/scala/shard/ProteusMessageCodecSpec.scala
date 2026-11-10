package shard

import _root_.proteus.ProtobufCodec
import shard.interfaces.MessageCodec
import shard.proteus.ProteusMessageCodec
import shard.proteus.given
import zio.Scope
import zio.blocks.schema.Schema
import zio.test._

object ProteusMessageCodecSpec extends ZIOSpecDefault {

  final case class Reply(text: String, ok: Boolean)
  final case class StreamReply(value: Int)
  final case class Input(delta: Int)
  final case class Page[A](items: List[A])
  final case class Track(id: String)
  final case class TrackItem(trackId: String)

  private given ProtobufCodec[Track] =
    ProtobufCodec.derived[TrackItem].transform(item => Track(item.trackId), track => TrackItem(track.id))

  sealed trait Error
  object Error {
    final case class NotFound(id: Int) extends Error
    final case class Invalid()         extends Error
  }

  enum Msg[E, A] {
    case Plain(name: String, count: Int) extends Msg[Nothing, Unit]
    case WithReply(id: Int)              extends Msg[Error, Reply]
    case WithStreamReply(from: Int)      extends Msg[Error.Invalid, Streamed[Error, StreamReply]]
    case WithDuplex()                    extends Msg[Nothing, Duplex[Input, Nothing, StreamReply]]
    case Generic(replies: List[Reply])   extends Msg[Nothing, Page[Reply]]
    case Singleton                       extends Msg[Nothing, Reply]
    case GetTrack(id: String)            extends Msg[Nothing, Track]
  }

  final case class Count(value: Int)

  private given ProtobufCodec[Count] = ProtobufCodec.derived[Int].transform(Count(_), _.value)

  enum NonMessage[E, A] {
    case Get() extends NonMessage[Nothing, Count]
  }

  // A reply type before and after a rolling update that adds a variant
  sealed trait OldStatus
  object OldStatus {
    final case class Active(since: Int) extends OldStatus
    final case class Unknown()          extends OldStatus
    given Schema[OldStatus] = Schema.derived[OldStatus].defaultValue(Unknown())
  }
  sealed trait NewStatus
  object NewStatus {
    final case class Active(since: Int)    extends NewStatus
    final case class Unknown()             extends NewStatus
    final case class Suspended(until: Int) extends NewStatus
  }
  final case class OldAccount(status: OldStatus)
  final case class NewAccount(status: NewStatus)

  enum OldMsg[E, A] {
    case GetStatus()  extends OldMsg[OldStatus, OldStatus]
    case GetAccount() extends OldMsg[Nothing, OldAccount]
  }
  enum NewMsg[E, A] {
    case GetStatus()  extends NewMsg[NewStatus, NewStatus]
    case GetAccount() extends NewMsg[Nothing, NewAccount]
  }

  private val codec = summon[MessageCodec[Msg]]

  private lazy val rendered = ProteusMessageCodec.dependency[Msg]("msg", "shard.test").render(Nil)

  private def roundtrip(msg: Msg[?, ?]): Msg[?, ?] =
    codec.decodeMessage(codec.encodeMessage(msg))

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ProteusMessageCodecSpec")(
      test("roundtrips messages") {
        val messages: List[Msg[?, ?]] = List(
          Msg.Plain("hello", 42),
          Msg.WithReply(7),
          Msg.WithStreamReply(3),
          Msg.WithDuplex(),
          Msg.Generic(List(Reply("a", true), Reply("b", false))),
          Msg.Singleton
        )
        assertTrue(messages.map(roundtrip) == messages)
      },
      test("decodes a variant it doesn't know as the default value of its type") {
        val (oldCodec, newCodec)     = (summon[MessageCodec[OldMsg]], summon[MessageCodec[NewMsg]])
        val (oldStatus, newStatus)   = (oldCodec.replyCodec(OldMsg.GetStatus()), newCodec.replyCodec(NewMsg.GetStatus()))
        val (oldAccount, newAccount) =
          (oldCodec.replyCodec(OldMsg.GetAccount()), newCodec.replyCodec(NewMsg.GetAccount()))
        val suspended                = NewStatus.Suspended(3)
        assertTrue(
          oldStatus.value.decode(newStatus.value.encode(suspended)) == OldStatus.Unknown(),
          oldStatus.error.decode(newStatus.error.encode(suspended)) == OldStatus.Unknown(),
          oldAccount.value.decode(newAccount.value.encode(NewAccount(suspended))) == OldAccount(OldStatus.Unknown())
        )
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
      test("renders the message with its reply, error, stream element, stream error and input types once") {
        val messages = List("Msg", "Reply", "Error", "NotFound", "Invalid", "StreamReply", "Input", "PageReply")
        assertTrue(
          messages.forall(name => rendered.contains(s"message $name {")),
          rendered.indexOf("message Error {") == rendered.lastIndexOf("message Error {")
        )
      },
      test("renders nothing for Unit and Nothing") {
        assertTrue(!rendered.contains("Unit"), !rendered.contains("Nothing"))
      },
      test("renders a transformed reply codec as the underlying message") {
        assertTrue(rendered.contains("message TrackItem {"), !rendered.contains("message Track {"))
      },
      test("rejects primitive reply types at compile time, and at declaration for an in-scope codec") {
        for {
          viaGiven  <- typeCheck {
                         """
                         enum Bad[E, A] {
                           case GetCount() extends Bad[Nothing, Int]
                         }
                         summon[shard.interfaces.MessageCodec[Bad]]
                         """
                       }
          // calling `derived` directly shows why
          viaDerive <- typeCheck {
                         """
                         enum Bad[E, A] {
                           case GetCount() extends Bad[Nothing, Int]
                         }
                         shard.proteus.ProteusMessageCodec.derived[Bad]
                         """
                       }
          viaOption <- typeCheck {
                         """
                         enum Bad[E, A] {
                           case Find() extends Bad[Nothing, Option[Reply]]
                         }
                         shard.proteus.ProteusMessageCodec.derived[Bad]
                         """
                       }
          viaList   <- typeCheck {
                         """
                         enum Bad[E, A] {
                           case All() extends Bad[List[Reply], Unit]
                         }
                         shard.proteus.ProteusMessageCodec.derived[Bad]
                         """
                       }
          viaCodec   = scala.util.Try(ProteusMessageCodec.derived[NonMessage])
        } yield assertTrue(
          viaGiven.isLeft,
          viaDerive.left.exists(_.contains("wrap primitive types")),
          viaOption.left.exists(_.contains("wrap primitive types")),
          viaList.left.exists(_.contains("wrap primitive types")),
          viaCodec.failed.toOption.exists(_.getMessage.contains("message or an enum"))
        )
      },
      test("rejects variants without fixed type arguments at compile time") {
        for {
          result <- typeCheck {
                      """
                      enum Bad[E, A] {
                        case Generic[A](reply: A) extends Bad[Nothing, A]
                      }
                      shard.proteus.ProteusMessageCodec.derived[Bad]
                      """
                    }
        } yield assertTrue(result.left.exists(_.contains("no fixed error or reply type")))
      }
    )
}
