package shard

import shard.interfaces.Storage
import sage.backend.{ subscribe, SageClient }
import sage.commands.Commands
import zio.json._
import zio.stream.ZStream
import zio.{ Task, ZIO, ZLayer }

object StorageRedis {

  /**
   * A layer that returns a Storage implementation using Redis
   */
  val live: ZLayer[SageClient & RedisConfig, Nothing, Storage] =
    ZLayer {
      for {
        config <- ZIO.service[RedisConfig]
        client <- ZIO.service[SageClient]
      } yield new Storage {
        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] =
          client
            .hGetAll[String, String](config.assignmentsKey(role))
            .map(_.flatMap { case (k, v) =>
              val pod = if (v.isEmpty) None else PodAddress(v)
              k.toIntOption.map(_ -> pod)
            })

        def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] = {
          val key = config.assignmentsKey(role)
          assignments.toList.map { case (k, v) => k.toString -> v.fold("")(_.toString) } match {
            case head :: tail => client.hSet(key, head, tail*) *> client.publish(key, "ping").unit
            case Nil          => client.publish(key, "ping").unit
          }
        }

        def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          client.subscribe[String](config.assignmentsKey(role)).mapZIO(_ => getAssignments(role))

        def getPods: Task[Map[PodAddress, Pod]] =
          client
            .hGetAll[String, String](config.podsKey)
            .map(_.toList.flatMap { case (k, v) =>
              PodAddress(k).flatMap(address => v.fromJson[Pod].toOption.map(address -> _))
            }.toMap)

        def savePods(pods: Map[PodAddress, Pod]): Task[Unit] =
          // replaces the whole hash atomically, so that the pods left out (unregistered) are removed
          pods.toList.map { case (k, v) => k.toString -> v.toJson } match {
            case head :: tail =>
              client
                .transaction(_.exec((Commands.del(config.podsKey), Commands.hSet(config.podsKey, head, tail*))))
                .unit
            case Nil          => client.del(config.podsKey).unit
          }
      }
    }
}
