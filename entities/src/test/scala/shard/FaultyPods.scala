package shard

import zio.{Config => _, _}
import zio.stream.ZStream

import shard.FaultyPods.Fault
import shard.errors.PodUnavailable
import shard.interfaces.{Pods, ShardManagerClient, Storage}
import shard.interfaces.Pods.{BinaryMessage, BinaryReply}

/**
  * Injects faults into the next calls to pods that send messages, one fault per call, in the order given.
  * The other calls, and the calls made while no fault is pending, go to the wrapped pods unchanged.
  * A killed pod is back at once, so a message sent again reaches it.
  */
final class FaultyPods private (faults: Queue[Fault]) {
  def inject(faults: Fault*): UIO[Unit] =
    this.faults.offerAll(faults).unit

  val pending: UIO[Int] =
    faults.size

  def wrap(pods: Pods): Pods =
    new Pods {
      export pods.{assignShards, ping, unassignShards}

      def sendMessage(pod: PodAddress, message: BinaryMessage): Task[BinaryReply] =
        faults.poll.flatMap {
          case None                                  => pods.sendMessage(pod, message)
          case Some(Fault.FailBeforeSending)         => ZIO.fail(Pods.NotSent(pod))
          case Some(Fault.DropAfterSending)          =>
            pods.sendMessage(pod, message).forkDaemon *> ZIO.fail(PodUnavailable(pod))
          case Some(Fault.KillWhileRunning(running)) =>
            pods.sendMessage(pod, message).interruptible.fork.flatMap(running *> _.interrupt) *> ZIO.fail(PodUnavailable(pod))
          case Some(Fault.KillAfterFinished)         => pods.sendMessage(pod, message) *> ZIO.fail(PodUnavailable(pod))
          case Some(Fault.NotHosted)                 => ZIO.succeed(BinaryReply.EntityNotManagedByThisPod)
          case Some(Fault.KillAfterAcknowledging)    => ZIO.succeed(BinaryReply.Success(Array.emptyByteArray))
          case Some(fault)                           => ZIO.dieMessage(s"$fault doesn't apply to sendMessage")
        }

      def sendStream(pod: PodAddress, messages: ZStream[Any, Nothing, BinaryMessage]): Task[BinaryReply] =
        faults.poll.flatMap {
          case None                          => pods.sendStream(pod, messages)
          // the messages are pulled, as by a transport that buffers them before connecting
          case Some(Fault.FailBeforeSending) => messages.runDrain *> ZIO.fail(Pods.NotSent(pod))
          case Some(Fault.NotHosted)         => ZIO.succeed(BinaryReply.EntityNotManagedByThisPod)
          case Some(Fault.DropAfter(n))      => pods.sendStream(pod, messages.take(n.toLong)) *> ZIO.fail(PodUnavailable(pod))
          case Some(fault)                   => ZIO.dieMessage(s"$fault doesn't apply to sendStream")
        }

      def sendStreamAndReceiveStream(
        pod: PodAddress,
        messages: ZStream[Any, Nothing, BinaryMessage]
      ): ZStream[Any, Throwable, BinaryReply] = {
        val replies = pods.sendStreamAndReceiveStream(pod, messages)
        ZStream.unwrap(faults.poll.map {
          case None                          => replies
          case Some(Fault.FailBeforeSending) => ZStream.fail(Pods.NotSent(pod))
          case Some(Fault.DropAfterSending)  => ZStream.fromZIO(replies.runDrain.forkDaemon) *> ZStream.fail(PodUnavailable(pod))
          case Some(Fault.KillAfterFinished) => ZStream.fromZIO(replies.runDrain) *> ZStream.fail(PodUnavailable(pod))
          case Some(Fault.NotHosted)         => ZStream.succeed(BinaryReply.EntityNotManagedByThisPod)
          case Some(fault)                   => ZStream.dieMessage(s"$fault doesn't apply to sendStreamAndReceiveStream")
        })
      }
    }
}

object FaultyPods {
  enum Fault {
    case FailBeforeSending
    // the message is delivered in the background, but the connection drops before the reply arrives,
    // so the message sent again may arrive before it
    case DropAfterSending
    // the pod dies once `running` completes, which interrupts the handler unless it is `Uninterruptible`
    case KillWhileRunning(running: UIO[Any])
    // the pod dies after the handler finished, before the reply is sent
    case KillAfterFinished
    case NotHosted
    // the pod acknowledges a message that expects no reply, then dies before running it
    case KillAfterAcknowledging
    // the first messages of a sendStream are delivered, then the connection drops
    case DropAfter(messages: Int)
  }

  def inject(faults: Fault*): URIO[FaultyPods, Unit] =
    ZIO.serviceWithZIO[FaultyPods](_.inject(faults*))

  val pending: URIO[FaultyPods, Int] =
    ZIO.serviceWithZIO[FaultyPods](_.pending)

  val localSharding: RLayer[ShardManagerClient & Storage & Config, Sharding & FaultyPods] =
    ZLayer
      .fromZIO(Queue.unbounded[Fault].map(new FaultyPods(_)))
      .flatMap(env => LocalSharding.withPods(env.get.wrap) ++ ZLayer.succeedEnvironment(env))
}
