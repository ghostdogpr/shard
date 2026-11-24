package shard.internal

import zio.{Exit, IO, Promise, Ref, Task, UIO, ZIO}
import zio.stream.ZStream

import shard.{AtLeastOnce, EntityOptions, Metrics, RecipientType, Role, ShardId}
import shard.errors.{EntityNotManagedByThisPod, MailboxFull, MessageDiscarded}
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{DeadLetter, Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply
import shard.interfaces.Pods.BinaryReply.DeliveryError

/**
  * Runs the `AtLeastOnce` messages of an entity type on this pod, once for each key.
  */
final private[shard] class AtLeastOnceDelivery[Msg[_, _]] private (
  recipientType: RecipientType[Msg],
  role: Role,
  storage: MessageStorage,
  options: EntityOptions[Msg],
  shardId: String => ShardId,
  isOwner: String => UIO[Boolean],
  inProgress: Ref[Map[Key, AtLeastOnceDelivery.Run]]
) {
  import AtLeastOnceDelivery.{Admission, Attempt, NoLongerStored, Run, RunError}

  type Enqueue = [E, A] => (entityId: String, message: Msg[E, A], isNew: Boolean) => Task[UIO[Attempt]]

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
    * Start the messages of these shards that are stored without a result and aren't running on this pod, and return how many are stored.
    */
  def recover(shards: Set[ShardId])(enqueue: Enqueue): Task[Long] =
    ZStream
      .suspend(storage.pending(role, recipientType.name, shards))
      .tap { (key, body) =>
        ZIO.whenZIODiscard(isOwner(key.entityId))(
          // a key that is gone since the scan read it was rejected or completed meanwhile, so the scan removes it again instead of running it
          start(key)(storeAndRun(key, body, enqueue)(ZIO.fail(NoLongerStored)))
            .flatMap {
              case Admission.Started(run) => run.accepted.await
              // even when a send started it, which reports its failure
              case Admission.Joined(run)  => run.accepted.await.ignore
            }
            .catchAllCause(cause =>
              cause.failureOption match {
                // the shard left this pod meanwhile, and its next owner runs the message, or it is a dead letter
                case Some(_: EntityNotManagedByThisPod) | Some(NoLongerStored) | Some(BinaryReply.Defect(_, Some(DeliveryError.DeadLettered))) =>
                  ZIO.unit
                case _                                                                                                                         =>
                  ZIO.logWarningCause(s"Failed to recover message ${key.deliveryKey} of ${key.entityType} ${key.entityId}", cause)
              }
            )
        )
      }
      .runCount

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
        Replies
          .decodeMessage(recipientType, stored)
          .foldZIO(
            error => save(key, DeadLetter(error.message)).map(ZIO.succeed(_)),
            (message, codec) => run(key, message, codec, isNew = false, enqueue)
          )
      case Stored.Done(result)    => outcome(result).map(ZIO.succeed(_))
    }

  private def run[E, A](
    key: Key,
    message: Msg[E, A],
    codec: ReplyCodec,
    isNew: Boolean,
    enqueue: Enqueue
  ): IO[RunError, IO[RunError, BinaryReply]] =
    options.retrySchedule.driver.flatMap { retries =>
      def queue(isNew: Boolean): IO[RunError, IO[RunError, BinaryReply]] =
        enqueue(key.entityId, message, isNew).map(_.flatMap(complete))

      // cut short by a rebalance or a shutdown: the message stays pending
      def whileOwner(otherwise: BinaryReply)(next: => IO[RunError, BinaryReply]): IO[RunError, BinaryReply] =
        isOwner(key.entityId).flatMap(owner => if (owner) next else ZIO.succeed(otherwise))

      def complete(attempt: Attempt): IO[RunError, BinaryReply] =
        attempt match {
          case Attempt.Ran(exit)        =>
            Replies.encodeExit(exit, codec) match {
              case result: (BinaryReply.Success | BinaryReply.Failure) => save(key, result)
              case BinaryReply.SerializationError(reason)              => save(key, DeadLetter(reason))
              case defect: BinaryReply.Defect                          =>
                whileOwner(defect)(
                  retries
                    .next(())
                    .foldZIO(_ => save(key, DeadLetter(s"Ran out of attempts: ${defect.message}")), _ => queue(isNew = false).flatten)
                )
              case other                                               => ZIO.succeed(other)
            }
          case Attempt.Skipped          =>
            whileOwner(BinaryReply.Interrupted)(queue(isNew = false).flatten)
          case Attempt.NotStarted(exit) => ZIO.succeed(Replies.encodeExit(exit, codec))
        }

      queue(isNew)
    }

  private def save(key: Key, result: Result): IO[BinaryReply.Defect, BinaryReply] =
    callStorage(storage.saveResult(role, key, result, options.retention))
      .tap {
        case deadLetter @ DeadLetter(reason) if deadLetter == result =>
          Metrics.deadLetters.tagged("role", role.name).tagged("type", key.entityType).increment *>
            ZIO.logWarning(s"Message ${key.deliveryKey} of ${key.entityType} ${key.entityId} became a dead letter: $reason")
        case _                                                       => ZIO.unit
      }
      .flatMap(outcome)

  // a dead letter fails, so that it also fails the admission of a sendDiscard of its key
  private def outcome(result: Result): IO[BinaryReply.Defect, BinaryReply] =
    result match {
      case DeadLetter(reason)  => Exit.fail(BinaryReply.Defect(reason, Some(DeliveryError.DeadLettered)))
      case result: BinaryReply => Exit.succeed(result)
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

  enum Attempt {
    case Ran(exit: Exit[Any, Any])
    case Skipped
    case NotStarted(exit: Exit[Any, Any])
  }

  def make[Msg[_, _]](
    recipientType: RecipientType[Msg],
    role: Role,
    storage: MessageStorage,
    options: EntityOptions[Msg],
    shardId: String => ShardId,
    isOwner: String => UIO[Boolean]
  ): UIO[AtLeastOnceDelivery[Msg]] =
    Ref
      .make(Map.empty[Key, Run])
      .map(new AtLeastOnceDelivery(recipientType, role, storage, options, shardId, isOwner, _))
}
