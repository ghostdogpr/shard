package shard.internal

import zio.{Duration, Exit, IO, Promise, Ref, Task, UIO, ZIO}
import zio.stream.ZStream

import shard.{AtLeastOnce, RecipientType, Role, ShardId}
import shard.errors.{EntityNotManagedByThisPod, MailboxFull, MessageDiscarded}
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
  shardId: String => ShardId,
  isOwner: String => UIO[Boolean],
  inProgress: Ref[Map[Key, AtLeastOnceDelivery.Run]]
) {
  import AtLeastOnceDelivery.{Admission, NoLongerStored, Run, RunError}

  type Enqueue = [E, A] => (entityId: String, message: Msg[E, A], isNew: Boolean) => Task[UIO[Exit[Any, Any]]]

  /**
    * Run a message once for its key, and return the reply for that key, or `Replies.ack` once it is in the mailbox.
    */
  def deliver[E, A](entityId: String, message: Msg[E, A] & AtLeastOnce, body: Array[Byte], codec: ReplyCodec, expectsReply: Boolean)(
    enqueue: Enqueue
  ): Task[BinaryReply] = {
    val key = Key(recipientType.name, shardId(entityId), entityId, message.atLeastOnceKey)
    start(key)(storeAndRun(key, body, enqueue)(run(key, message, codec, isNew = true, enqueue)))
      .flatMap(admission => if (expectsReply) admission.run.result.await else admission.run.accepted.await.as(Replies.ack))
      .catchAll {
        // it joined a scan that found its key gone, so it runs as a new message
        case NoLongerStored             => deliver(entityId, message, body, codec, expectsReply)(enqueue)
        case defect: BinaryReply.Defect => ZIO.succeed(defect)
        case error: Throwable           => ZIO.fail(error)
      }
  }

  /**
    * Start the messages of these shards that are stored without a result and aren't running on this pod.
    */
  def recover(shards: Set[ShardId])(enqueue: Enqueue): Task[Unit] =
    ZStream.suspend(storage.pending(role, recipientType.name, shards)).runForeach { (key, body) =>
      ZIO.whenZIODiscard(isOwner(key.entityId))(
        // a key that is gone since the scan read it was rejected or completed meanwhile, so the scan removes it again instead of running it
        start(key)(storeAndRun(key, body, enqueue)(ZIO.fail(NoLongerStored)))
          .flatMap {
            case Admission.Started(run) => run.accepted.await
            case Admission.Joined(_)    => ZIO.unit
          }
          .catchAllCause(cause =>
            cause.failureOption match {
              // the shard left this pod meanwhile, and its next owner runs the message
              case Some(_: EntityNotManagedByThisPod) | Some(NoLongerStored) => ZIO.unit
              case _                                                         =>
                ZIO.logWarningCause(s"Failed to recover message ${key.deliveryKey} of ${key.entityType} ${key.entityId}", cause)
            }
          )
      )
    }

  private def start(key: Key)(admit: => IO[RunError, IO[RunError, BinaryReply]]): UIO[Admission] =
    (Promise.make[RunError, Unit] <*> Promise.make[RunError, BinaryReply]).flatMap { (accepted, result) =>
      inProgress
        .modify(map =>
          map.get(key) match {
            case Some(running) => (ZIO.succeed(Admission.Joined(running)), map)
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
              (started.as(Admission.Started(running)), map.updated(key, running))
          }
        )
        .flatten
        .uninterruptible
    }

  private def storeAndRun(key: Key, body: Array[Byte], enqueue: Enqueue)(
    whenNew: => IO[RunError, IO[RunError, BinaryReply]]
  ): IO[RunError, IO[RunError, BinaryReply]] =
    callStorage(storage.store(role, key, body)).flatMap[Any, RunError, IO[RunError, BinaryReply]] {
      case Stored.New             =>
        whenNew.tapSomeError {
          // so that a message that this pod rejected before it is in the mailbox can be sent again,
          // while one whose shard left this pod stays pending for the next owner
          case _: MailboxFull | _: MessageDiscarded | NoLongerStored => callStorage(storage.remove(role, key))
        }
      case Stored.Pending(stored) =>
        Replies.decodeMessage(recipientType, stored).flatMap((message, codec) => run(key, message, codec, isNew = false, enqueue))
      case Stored.Done(result)    => ZIO.succeed(ZIO.succeed(result))
    }

  private def run[E, A](key: Key, message: Msg[E, A], codec: ReplyCodec, isNew: Boolean, enqueue: Enqueue): IO[RunError, IO[RunError, BinaryReply]] =
    enqueue(key.entityId, message, isNew).map(saveResult(key, codec, _))

  private def saveResult(key: Key, codec: ReplyCodec, result: UIO[Exit[Any, Any]]): IO[BinaryReply.Defect, BinaryReply] =
    result.flatMap { exit =>
      Replies.encodeExit(exit, codec) match {
        case result: Result => callStorage(storage.saveResult(role, key, result, retention))
        case reply          => ZIO.succeed(reply)
      }
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

  // a scan found the key of a message gone
  private case object NoLongerStored

  private type RunError = Throwable | BinaryReply.Defect | NoLongerStored.type

  final private case class Run(accepted: Promise[RunError, Unit], result: Promise[RunError, BinaryReply])

  private enum Admission {
    def run: Run
    case Started(run: Run)
    case Joined(run: Run)
  }

  def make[Msg[_, _]](
    recipientType: RecipientType[Msg],
    role: Role,
    storage: MessageStorage,
    retention: Duration,
    shardId: String => ShardId,
    isOwner: String => UIO[Boolean]
  ): UIO[AtLeastOnceDelivery[Msg]] =
    Ref
      .make(Map.empty[Key, Run])
      .map(new AtLeastOnceDelivery(recipientType, role, storage, retention, shardId, isOwner, _))
}
