package shard

import shard.interfaces.Storage
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.data.RedisChannel
import dev.profunktor.redis4cats.effects.ScriptOutputType
import dev.profunktor.redis4cats.pubsub.PubSubCommands
import zio.stream.ZStream
import zio.stream.interop.fs2z._
import zio.{ Task, ZIO, ZLayer }

object StorageRedis {
  type fs2Stream[A] = fs2.Stream[Task, A]
  type Redis        = RedisCommands[Task, String, String] & PubSubCommands[Task, fs2Stream, String, String]

  private val replaceScript: String =
    """redis.call('DEL', KEYS[1])
      |for i = 1, #ARGV, 2 do
      |  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
      |end
      |return redis.status_reply('OK')""".stripMargin

  /**
   * A layer that returns a Storage implementation using Redis
   */
  val live: ZLayer[Redis & RedisConfig, Nothing, Storage] =
    ZLayer {
      for {
        config       <- ZIO.service[RedisConfig]
        stringClient <- ZIO.service[RedisCommands[Task, String, String]]
        pubSubClient <- ZIO.service[PubSubCommands[Task, fs2Stream, String, String]]
      } yield new Storage {
        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] =
          stringClient
            .hGetAll(config.assignmentsKey(role))
            .map(_.flatMap { case (k, v) =>
              val pod = if (v.isEmpty) None else PodAddress(v)
              k.toIntOption.map(_ -> pod)
            })

        def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] = {
          val key = config.assignmentsKey(role)
          // Lettuce rejects an HSET without fields
          stringClient
            .hSet(key, assignments.map { case (k, v) => k.toString -> v.fold("")(_.toString) })
            .whenDiscard(assignments.nonEmpty) *>
            pubSubClient.publish(RedisChannel(key), "ping").unit
        }

        def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          pubSubClient
            .subscribe(RedisChannel(config.assignmentsKey(role)))
            .toZStream(1)
            .mapZIO(_ => getAssignments(role))

        def getPods: Task[Set[Pod]] =
          stringClient
            .hGetAll(config.podsKey)
            .map(_.flatMap { case (k, v) => PodAddress(k).flatMap(Pod.decode(_, v)) }.toSet)

        def savePods(pods: Set[Pod]): Task[Unit] =
          // one script, so that the hash is replaced atomically and the pods left out (unregistered) are removed
          stringClient
            .eval(
              replaceScript,
              ScriptOutputType.Status[String],
              List(config.podsKey),
              pods.toList.flatMap(pod => List(pod.address.toString, Pod.encode(pod)))
            )
            .unit
      }
    }
}
