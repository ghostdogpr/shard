package shard

import zio.{Promise, RLayer, Task, ZIO, ZLayer}
import zio.stream.ZStream

import shard.interfaces.{MessageStorage, Pods, ShardManagerClient, Storage}
import shard.interfaces.Pods.{BinaryMessage, BinaryReply}

object LocalSharding {

  /**
    * Create pods that deliver every message to the local `Sharding`.
    * The `Sharding` is passed as a promise since it is built from these pods.
    */
  private def localPods(sharding: Promise[Nothing, Sharding]): Pods =
    new Pods {
      def assignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit]   = ZIO.unit
      def unassignShards(pod: PodAddress, role: Role, shards: Set[ShardId]): Task[Unit] = ZIO.unit
      def ping(pod: PodAddress): Task[Unit]                                             = ZIO.unit

      def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply] =
        sharding.await.flatMap(_.sendToLocalEntity(message))

      def sendStream(pod: PodAddress, messages: ZStream[Any, Nothing, BinaryMessage]): Task[BinaryReply] =
        sharding.await.flatMap(_.sendStreamToLocalEntity(messages))

      def sendStreamAndReceiveStream(
        pod: PodAddress,
        messages: ZStream[Any, Nothing, BinaryMessage]
      ): ZStream[Any, Throwable, BinaryReply] =
        ZStream.unwrap(sharding.await.map(_.sendStreamToLocalEntityAndReceiveStream(messages)))
    }

  /**
    * A special layer meant for testing that delivers messages locally rather than with an external transport.
    * This layer will only work in a single JVM and is not suitable for production use.
    */
  val live: RLayer[ShardManagerClient & Storage & MessageStorage & Config, Sharding] =
    withPods(identity)

  private[shard] def withPods(wrap: Pods => Pods): RLayer[ShardManagerClient & Storage & MessageStorage & Config, Sharding] =
    ZLayer.fromZIO(Promise.make[Nothing, Sharding]).flatMap { env =>
      val sharding = env.get
      ZLayer
        .makeSome[ShardManagerClient & Storage & MessageStorage & Config, Sharding](
          ZLayer.succeed(wrap(localPods(sharding))),
          Sharding.live
        )
        .tap(env => sharding.succeed(env.get))
    }
}
