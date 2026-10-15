package shard

import shard.interfaces.MessageCodec
import shard.kryo.given
import zio.Scope
import zio.test._

object KryoMessageCodecSpec extends ZIOSpecDefault {

  enum Msg[+E, +A] {
    case Plain(name: String, count: Int) extends Msg[Nothing, Unit]
    case WithReply(id: Int)              extends Msg[String, Int]
    case WithStreamReply()               extends Msg[Nothing, Streamed[String, Int]]
  }

  private val codec = summon[MessageCodec[Msg]]

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("KryoMessageCodecSpec")(
      test("roundtrips messages") {
        val messages: List[Msg[Any, Any]] = List(Msg.Plain("hello", 42), Msg.WithReply(7), Msg.WithStreamReply())
        assertTrue(messages.map(msg => codec.decodeMessage(codec.encodeMessage(msg))) == messages)
      },
      test("roundtrips replies and errors") {
        val replyCodec = codec.replyCodec(Msg.WithReply(7))
        assertTrue(
          replyCodec.value.decode(replyCodec.value.encode(42)) == 42,
          replyCodec.error.decode(replyCodec.error.encode("error")) == "error"
        )
      },
      test("roundtrips stream elements and errors") {
        val replyCodec = codec.replyCodec(Msg.WithStreamReply())
        assertTrue(
          replyCodec.value.decode(replyCodec.value.encode(99)) == 99,
          replyCodec.streamError.decode(replyCodec.streamError.encode("error")) == "error"
        )
      }
    )
}
