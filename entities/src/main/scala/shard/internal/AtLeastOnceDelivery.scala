package shard.internal

import zio.{Duration, Exit, IO, Promise, Ref, Task, UIO, ZIO}

import shard.{AtLeastOnce, RecipientType, Role}
import shard.errors.{MailboxFull, MessageDiscarded}
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply
import shard.interfaces.Pods.BinaryReply.DeliveryError

/**
  * Runs the `AtLeastOnce` messages of an entity type on this pod, once for each key.
  */
final private[shard] class AtLeastOnceDelivery[Msg[_, _]] private (
  recipientType: RecipientType[Msg],
  role: Role,
  storage: MessageStorage,
  retention: Duration,
  inProgress: Ref[Map[Key, AtLeastOnceDelivery.Run]]
) {
  import AtLeastOnceDelivery.{Run, RunError}

  type Enqueue = [E, A] => (message: Msg[E, A], useLoadEntity: Boolean) => Task[UIO[Exit[Any, Any]]]

  /**
    * Run a message once for its key, and return the reply for that key, or `Replies.ack` once it is in the mailbox.
    */
  def deliver[E, A](entityId: String, message: Msg[E, A] & AtLeastOnce, body: Array[Byte], codec: ReplyCodec, expectsReply: Boolean)(
    enqueue: Enqueue
  ): Task[BinaryReply] = {
    val key = Key(recipientType.name, entityId, message.atLeastOnceKey)
    startOrJoin(key)(storeAndRun(key, message, body, codec, enqueue))
      .flatMap(run => if (expectsReply) run.result.await else run.accepted.await.as(Replies.ack))
      .catchAll(replyStorageFailure)
  }

  private def startOrJoin(key: Key)(admit: => IO[RunError, IO[RunError, BinaryReply]]): UIO[Run] =
    (Promise.make[RunError, Unit] <*> Promise.make[RunError, BinaryReply]).flatMap { (accepted, result) =>
      inProgress
        .modify(map =>
          map.get(key) match {
            case Some(running) => (ZIO.succeed(running), map)
            case None          =>
              // interruptible even for an uninterruptible caller, so that timeouts in the storage work
              // the key is removed first, so that a caller retrying after this run starts a new one
              val started = admit
                .flatMap(accepted.succeed(()) *> _)
                .interruptible
                .exit
                .flatMap(exit => inProgress.update(_ - key) *> accepted.complete(exit.unit) *> result.done(exit))
                .forkDaemon
              val running = Run(accepted, result)
              (started.as(running), map.updated(key, running))
          }
        )
        .flatten
        .uninterruptible
    }

  private def storeAndRun[E, A](
    key: Key,
    message: Msg[E, A],
    body: Array[Byte],
    codec: ReplyCodec,
    enqueue: Enqueue
  ): IO[RunError, IO[RunError, BinaryReply]] =
    callStorage(storage.store(role, key, body)).flatMap[Any, RunError, IO[RunError, BinaryReply]] {
      case Stored.New             =>
        enqueue(message, true)
          .catchAll {
            // so that a message that this pod rejected before it is in the mailbox can be sent again,
            // while one whose shard left this pod stays pending for the next owner
            case error @ (_: MailboxFull | _: MessageDiscarded) => callStorage(storage.remove(role, key)) *> ZIO.fail(error)
            case error                                          => ZIO.fail(error)
          }
          .map(saveResult(key, codec, _))
      case Stored.Pending(stored) =>
        Replies
          .decodeMessage(recipientType, stored)
          .flatMap((decoded, storedCodec) => enqueue(decoded, false).map(saveResult(key, storedCodec, _)))
      case Stored.Done(result)    => ZIO.succeed(ZIO.succeed(result))
    }

  private def saveResult(key: Key, codec: ReplyCodec, result: UIO[Exit[Any, Any]]): IO[BinaryReply.Defect, BinaryReply] =
    result.flatMap { exit =>
      Replies.encodeExit(exit, codec) match {
        case result: Result => callStorage(storage.saveResult(role, key, result, retention))
        case reply          => ZIO.succeed(reply)
      }
    }

  private def replyStorageFailure(error: RunError): Task[BinaryReply] =
    error match {
      case defect: BinaryReply.Defect => ZIO.succeed(defect)
      case error: Throwable           => ZIO.fail(error)
    }

  private def callStorage[A](effect: => Task[A]): IO[BinaryReply.Defect, A] =
    ZIO
      .suspendSucceed(effect)
      .catchAllCause(cause =>
        ZIO.fail(cause.dieOption match {
          case Some(throwable) =>
            BinaryReply.Defect(s"Message storage died with ${throwable.getClass.getName}: ${throwable.getMessage}")
          case None            =>
            BinaryReply.Defect(s"Message storage is unavailable: ${cause.squash}", Some(DeliveryError.StorageUnavailable))
        })
      )
}

private[shard] object AtLeastOnceDelivery {

  private type RunError = Throwable | BinaryReply.Defect

  final private case class Run(accepted: Promise[RunError, Unit], result: Promise[RunError, BinaryReply])

  def make[Msg[_, _]](
    recipientType: RecipientType[Msg],
    role: Role,
    storage: MessageStorage,
    retention: Duration
  ): UIO[AtLeastOnceDelivery[Msg]] =
    Ref
      .make(Map.empty[Key, Run])
      .map(new AtLeastOnceDelivery(recipientType, role, storage, retention, _))
}
