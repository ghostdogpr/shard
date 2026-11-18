package example.simple

import scala.annotation.unused

import zio.{IO, Ref, UIO, ZIO}
import zio.stream.ZStream

import shard.{EntityType, Streamed}
import shard.javaSerialization.given

object GuildBehavior {
  case class GuildFull(maxMembers: Int)

  enum GuildMessage[E, A] {
    case Join(userId: String)  extends GuildMessage[GuildFull, Set[String]]
    case Timeout()             extends GuildMessage[Nothing, Set[String]]
    case Leave(userId: String) extends GuildMessage[Nothing, Unit]
    case Stream()              extends GuildMessage[Nothing, Streamed[Nothing, String]]
  }

  object Guild extends EntityType[GuildMessage]("guild")

  def behavior(@unused entityId: String): UIO[Guild.Handler] =
    Ref.make(Set.empty[String]).map { state =>
      new Guild.Handler {
        def handle[E, A](message: GuildMessage[E, A]): IO[E, A] =
          message match {
            case GuildMessage.Join(userId)  =>
              state.get.flatMap(members =>
                if (members.size >= 5) ZIO.fail(GuildFull(5))
                else state.updateAndGet(_ + userId)
              )
            case GuildMessage.Leave(userId) =>
              state.update(_ - userId)
            case GuildMessage.Timeout()     =>
              ZIO.never // simulate a timeout by not responding
            case GuildMessage.Stream() =>
              state.get.map(members => Streamed(ZStream.fromIterable(members)))
          }
      }
    }
}
