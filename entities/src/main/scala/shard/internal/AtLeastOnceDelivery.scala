package shard.internal

import zio.{Duration, Exit, Promise, Ref, Task, UIO, ZIO}

import shard.{AtLeastOnce, RecipientType, Role}
import shard.errors.{MailboxFull, MessageDiscarded}
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply
import shard.interfaces.Pods.BinaryReply.DeliveryError

/**
  * Runs the `AtLeastOnce` messages of an entity type on this pod, once for each key.
  * A message is stored before it runs and its result is saved after it: a message whose key has a result gets it
  * without running, and a message whose key is running on this pod waits for that run.
  */
final private[shard] class AtLeastOnceDelivery[Msg[_, _]] private (
  recipientType: RecipientType[Msg],
  role: Role,
  storage: MessageStorage,
  retention: Duration,
  inProgress: Ref[Map[Key, Promise[Throwable, BinaryReply]]]
) {

  /**
    * Add a message to the entity's mailbox and wait for its result.
    */
  type Ask = [E, A] => Msg[E, A] => Task[Exit[Any, Any]]

  /**
    * Run a message once for its key and return the reply for that key, encoded with `codec` if this message runs.
    * The run goes on if the caller is interrupted, so that its result is saved.
    */
  def runOnce[E, A](entityId: String, message: Msg[E, A] & AtLeastOnce, body: Array[Byte], codec: ReplyCodec)(
    ask: Ask
  ): Task[BinaryReply] = {
    val key = Key(recipientType.name, entityId, message.atLeastOnceKey)
    Promise.make[Throwable, BinaryReply].flatMap { promise =>
      ZIO.uninterruptibleMask(restore =>
        inProgress
          .modify(map =>
            map.get(key) match {
              case Some(running) => (restore(running.await), map)
              case None          =>
                // the run itself is interruptible, even for an uninterruptible caller, so that timeouts in the storage work
                val run = storeAndRun(key, message, body, codec, ask).interruptible
                  .onExit(exit => inProgress.update(_ - key) *> promise.done(exit))
                  .forkDaemon
                (run *> restore(promise.await), map.updated(key, promise))
            }
          )
          .flatten
      )
    }
  }

  private def storeAndRun[E, A](key: Key, message: Msg[E, A], body: Array[Byte], codec: ReplyCodec, ask: Ask): Task[BinaryReply] =
    callStorage(storage.store(role, key, body)) {
      case Stored.New             =>
        runAndSaveResult(key, message, codec, ask).catchAll {
          // so that a message that this pod rejected before running can be sent again,
          // while one whose shard left this pod stays pending for the next owner
          case error @ (_: MailboxFull | _: MessageDiscarded) => callStorage(storage.remove(role, key))(_ => ZIO.fail(error))
          case error                                          => ZIO.fail(error)
        }
      case Stored.Pending(stored) => Replies.decodeMessage(recipientType, stored).flatMap(runAndSaveResult(key, _, _, ask))
      case Stored.Done(result)    => ZIO.succeed(result)
    }

  private def runAndSaveResult[E, A](key: Key, message: Msg[E, A], codec: ReplyCodec, ask: Ask): Task[BinaryReply] =
    ask(message).flatMap { exit =>
      Replies.encodeExit(exit, codec) match {
        // the reply is the result kept in storage, which is another run's if it saved first
        case result: Result => callStorage(storage.saveResult(role, key, result, retention))(ZIO.succeed(_))
        // a defect, an interruption or a result that can't be encoded leaves the message without a result
        case reply          => ZIO.succeed(reply)
      }
    }

  private def callStorage[A](effect: => Task[A])(f: A => Task[BinaryReply]): Task[BinaryReply] =
    ZIO
      .suspendSucceed(effect)
      .foldCauseZIO(
        cause =>
          ZIO.succeed(cause.dieOption match {
            case Some(throwable) =>
              BinaryReply.Defect(s"Message storage died with ${throwable.getClass.getName}: ${throwable.getMessage}")
            case None            =>
              BinaryReply.Defect(s"Message storage is unavailable: ${cause.squash}", Some(DeliveryError.StorageUnavailable))
          }),
        f
      )
}

private[shard] object AtLeastOnceDelivery {
  def make[Msg[_, _]](
    recipientType: RecipientType[Msg],
    role: Role,
    storage: MessageStorage,
    retention: Duration
  ): UIO[AtLeastOnceDelivery[Msg]] =
    Ref
      .make(Map.empty[Key, Promise[Throwable, BinaryReply]])
      .map(new AtLeastOnceDelivery(recipientType, role, storage, retention, _))
}
