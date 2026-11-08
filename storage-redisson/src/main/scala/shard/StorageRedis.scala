package shard

import scala.jdk.CollectionConverters._

import shard.interfaces.Storage
import org.redisson.api.BatchOptions.ExecutionMode
import org.redisson.api.{ BatchOptions, RedissonClient }
import org.redisson.api.listener.MessageListener
import org.redisson.client.codec.StringCodec
import zio.stream.ZStream
import zio.{ Queue, Task, Unsafe, ZIO, ZLayer }

object StorageRedis {

  /**
   * A layer that returns a Storage implementation using Redis
   */
  val live: ZLayer[RedissonClient & RedisConfig, Nothing, Storage] =
    ZLayer {
      for {
        config      <- ZIO.service[RedisConfig]
        redisClient <- ZIO.service[RedissonClient]
        podsMap      = redisClient.getMap[String, String](config.podsKey, StringCodec.INSTANCE)
      } yield new Storage {
        def getAssignments(role: Role): Task[Map[ShardId, Option[PodAddress]]] = {
          val assignmentsMap = redisClient.getMap[String, String](config.assignmentsKey(role), StringCodec.INSTANCE)
          ZIO
            .fromCompletionStage(assignmentsMap.readAllMapAsync())
            .map(_.asScala.toMap.flatMap { case (k, v) =>
              val pod = if (v.isEmpty) None else PodAddress(v)
              k.toIntOption.map(_ -> pod)
            })
        }

        def saveAssignments(role: Role, assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] = {
          val assignmentsMap   = redisClient.getMap[String, String](config.assignmentsKey(role), StringCodec.INSTANCE)
          val assignmentsTopic = redisClient.getTopic(config.assignmentsKey(role), StringCodec.INSTANCE)
          ZIO.fromCompletionStage(assignmentsMap.putAllAsync(assignments.map { case (k, v) =>
            k.toString -> v.fold("")(_.toString)
          }.asJava)) *>
            ZIO.fromCompletionStage(assignmentsTopic.publishAsync("ping")).unit
        }

        def assignmentsStream(role: Role): ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          ZStream.unwrapScoped {
            val assignmentsTopic = redisClient.getTopic(config.assignmentsKey(role), StringCodec.INSTANCE)
            for {
              queue   <- ZIO.acquireRelease(Queue.unbounded[String])(_.shutdown)
              runtime <- ZIO.runtime[Any]
              // removed when the stream ends, before the queue shuts down
              _       <- ZIO.acquireRelease(
                           ZIO.fromCompletionStage(
                             assignmentsTopic.addListenerAsync(
                               classOf[String],
                               new MessageListener[String] {
                                 def onMessage(channel: CharSequence, msg: String): Unit =
                                   Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(queue.offer(msg)))
                               }
                             )
                           )
                         )(id => ZIO.fromCompletionStage(assignmentsTopic.removeListenerAsync(id)).ignoreLogged)
            } yield ZStream.fromQueue(queue).mapZIO(_ => getAssignments(role))
          }

        def getPods: Task[Set[Pod]] =
          ZIO
            .fromCompletionStage(podsMap.readAllMapAsync())
            .map(_.asScala.flatMap { case (k, v) => PodAddress(k).flatMap(Pod.decode(_, v)) }.toSet)

        def savePods(pods: Set[Pod]): Task[Unit] =
          // replaces the whole map atomically, so that the pods left out (unregistered) are removed
          ZIO.fromCompletionStage {
            val batch = redisClient.createBatch(BatchOptions.defaults().executionMode(ExecutionMode.IN_MEMORY_ATOMIC))
            val map   = batch.getMap[String, String](config.podsKey, StringCodec.INSTANCE)
            map.deleteAsync()
            map.putAllAsync(pods.map(pod => pod.address.toString -> Pod.encode(pod)).toMap.asJava)
            batch.executeAsync()
          }.unit
      }
    }
}
