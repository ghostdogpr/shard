package example.complex

import shard.EntityType
import shard.kryo.given
import sage.backend.SageClient
import zio.{ IO, Scope, URIO, ZIO }

object GuildBehavior {
  case class GuildFull(maxMembers: Int)

  enum GuildMessage[+E, +A] {
    case Join(userId: String)  extends GuildMessage[GuildFull, Set[String]]
    case Leave(userId: String) extends GuildMessage[Nothing, Unit]
  }

  object Guild extends EntityType[GuildMessage]("guild")

  def behavior(entityId: String): URIO[SageClient & Scope, Guild.Handler] =
    for {
      redis <- ZIO.service[SageClient]
      _     <- ZIO.logInfo(s"Started entity $entityId")
      // runs when the entity stops, e.g. when its shard moves to another pod
      _     <- ZIO.addFinalizer(ZIO.logInfo(s"Stopped entity $entityId"))
    } yield new Guild.Handler {
      def handle[E, A](message: GuildMessage[E, A]): IO[E, A] =
        message match {
          case GuildMessage.Join(userId)  =>
            redis
              .lRange[String](entityId, 0, -1)
              .map(_.toSet)
              .orDie
              .flatMap(members =>
                if (members.contains(userId)) ZIO.succeed(members)
                else if (members.size >= 5) ZIO.fail(GuildFull(5))
                else redis.lPush(entityId, userId).orDie.as(members + userId)
              )
          case GuildMessage.Leave(userId) =>
            redis.lRem(entityId, 1, userId).orDie.unit
        }
    }
}
