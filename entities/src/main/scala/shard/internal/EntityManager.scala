package shard.internal

import shard._
import shard.errors.{ EntityNotManagedByThisPod, MailboxFull, MessageDiscarded, StreamCancelled }
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.Pods.BinaryMessage
import zio.{ Config => _, _ }
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

private[shard] object EntityManager {

  /**
   * The reply of a Streamed or Duplex message: runs its stream given its inputs (ignored for Streamed).
   *
   * @param usesInput false for Streamed, whose stream never runs its inputs
   */
  final case class StreamReply(usesInput: Boolean, run: ZStream[Any, Nothing, Any] => ZStream[Any, Any, Any])

  private sealed trait EnvelopeState
  private object EnvelopeState {
    case object Queued                                           extends EnvelopeState
    final case class Running(fiber: Fiber.Runtime[Nothing, Any]) extends EnvelopeState
    // completed, or cancelled by the sender
    case object Done                                             extends EnvelopeState
  }

  /**
   * A message enqueued for an entity.
   */
  final class Envelope[Msg[_, _]] private[EntityManager] (
    val message: Msg[Any, Any],
    private[EntityManager] val result: Promise[Nothing, Exit[Any, Any]],
    private[EntityManager] val state: Ref[EnvelopeState]
  ) {

    private[EntityManager] def cancel: UIO[Unit] =
      if (message.isInstanceOf[Uninterruptible]) ZIO.unit
      else
        state.modify {
          case EnvelopeState.Queued         => (ZIO.unit, EnvelopeState.Done)
          case EnvelopeState.Running(fiber) => (fiber.interruptFork, EnvelopeState.Done)
          case other                        => (ZIO.unit, other)
        }.flatten
  }

  private object Envelope {
    def make[Msg[_, _]](message: Msg[Any, Any]): UIO[Envelope[Msg]] =
      for {
        result <- Promise.make[Nothing, Exit[Any, Any]]
        state  <- Ref.make[EnvelopeState](EnvelopeState.Queued)
      } yield new Envelope(message, result, state)
  }

  /**
   * @param drained completed when the entity is closing and has no pending messages or open streams, or a message died
   * @param closing completed when the entity starts closing, to interrupt its streams
   */
  private final class RunningEntity[Msg[_, _]](
    val entityId: String,
    val mailbox: Queue[Envelope[Msg]],
    val concurrentMailbox: Queue[Envelope[Msg]],
    val state: Ref[RunningEntity.State],
    val drained: Promise[Nothing, Unit],
    val closing: Promise[Nothing, Unit],
    val terminated: Promise[Nothing, Unit]
  )

  private object RunningEntity {

    /**
     * @param pending number of messages enqueued and not yet completed, limited by `mailboxCapacity`
     * @param streams number of open reply streams, which don't count in `pending` but keep the entity running
     */
    final case class State(
      pending: Int,
      streams: Int,
      closing: Boolean,
      lastActive: EpochMillis
    ) {
      def isIdle: Boolean = pending == 0 && streams == 0
    }
  }

  // above that number, the concurrency is considered unbounded and no permits are used
  private val maxBoundedConcurrency = 65536

  private val currentTimeInMilliseconds: UIO[EpochMillis] =
    Clock.currentTime(TimeUnit.MILLISECONDS)

  private val stoppingRetryDelay: UIO[Unit] =
    Clock.sleep(100.millis)

  def make[Msg[_, _]](
    recipientType: RecipientType[Msg],
    makeHandler: String => URIO[Scope, EntityHandler[Msg]],
    sharding: Sharding,
    config: Config,
    options: EntityOptions[Msg]
  ): URIO[Scope, EntityManager[Msg]] =
    for {
      entities   <- Ref.Synchronized.make[Map[String, RunningEntity[Msg]]](Map())
      closed     <- Ref.make(false)
      registered <- ZIO.getFiberRefs
      manager     = new EntityManager(recipientType, makeHandler, entities, closed, registered, sharding, config, options)
      _          <- manager.reaper.interruptible.forkScoped
    } yield manager
}

/**
 * Runs the entities of an entity or topic type on this pod.
 * Only the processing of a message is interruptible, so that no message is lost or left uncompleted.
 */
private[shard] final class EntityManager[Msg[_, _]] private (
  val recipientType: RecipientType[Msg],
  makeHandler: String => URIO[Scope, EntityHandler[Msg]],
  entities: Ref.Synchronized[Map[String, EntityManager.RunningEntity[Msg]]],
  closed: Ref[Boolean],
  registered: FiberRefs,
  sharding: Sharding,
  config: Config,
  options: EntityOptions[Msg]
) {
  import EntityManager._

  private val gauge = Metrics.entities.tagged("role", config.role.name).tagged("type", recipientType.name)

  private val interrupted: Exit[Any, Any] = Exit.interrupt(FiberId.None)

  private val sequential = options.concurrency <= 1

  private val interceptor = config.entityInterceptor ++ options.interceptor

  val reaper: UIO[Nothing] = {
    val maxIdleTime   = options.maxIdleTime getOrElse config.entityMaxIdleTime
    val maxIdleMillis = maxIdleTime.toMillis
    val interval      = maxIdleTime.dividedBy(10L).max(10.millis).min(5.seconds)
    val reap          =
      for {
        now     <- currentTimeInMilliseconds
        running <- entities.get
        _       <- ZIO.foreachDiscard(running.values)(
                     stopInBackground(_, s => s.isIdle && now - s.lastActive >= maxIdleMillis)
                   )
      } yield ()
    (Clock.sleep(interval) *> reap).forever
  }

  private def isOnLocalShards(entityId: String): UIO[Boolean] =
    recipientType match {
      case _: EntityType[?] => sharding.isEntityOnLocalShards(recipientType, entityId)
      case _: TopicType[?]  => ZIO.succeed(true)
    }

  /**
   * Add a message to an entity's mailbox, starting the entity if needed, and trying again a little later if it is stopping.
   * Fails with `EntityNotManagedByThisPod`, `MailboxFull` or `MessageDiscarded`.
   */
  def enqueue[E, A](entityId: String, message: Msg[E, A]): IO[Throwable, Envelope[Msg]] =
    tryEnqueue(entityId, message).someOrElseZIO(stoppingRetryDelay *> enqueue(entityId, message))

  private def tryEnqueue[E, A](entityId: String, message: Msg[E, A]): IO[Throwable, Option[Envelope[Msg]]] = {
    val erased = message.asInstanceOf[Msg[Any, Any]]
    for {
      _        <- ZIO.unlessZIO(isOnLocalShards(entityId))(ZIO.fail(EntityNotManagedByThisPod(entityId)))
      map      <- entities.get
      envelope <- map.get(entityId) match {
                    case Some(entity)                         => tryOffer(entity, erased)
                    case None if !options.loadEntity(message) =>
                      ZIO.fail(MessageDiscarded(recipientType.name, entityId))
                    case None                                 => getOrStart(entityId).flatMap(tryOffer(_, erased))
                  }
    } yield envelope
  }

  /**
   * Add a message to an entity's mailbox and wait for its result.
   * If the caller is interrupted while waiting, the message is cancelled.
   */
  def ask[E, A](entityId: String, message: Msg[E, A]): IO[Throwable, Exit[Any, Any]] =
    ZIO.uninterruptibleMask(restore =>
      tryEnqueue(entityId, message).flatMap {
        case Some(envelope) => restore(envelope.result.await).onInterrupt(envelope.cancel)
        case None           => restore(stoppingRetryDelay *> ask(entityId, message))
      }
    )

  /**
   * Deliver a message from another pod and give its result to `onExit` with the codec of its reply.
   * A message that expects no reply returns `ack` once it is in the mailbox.
   */
  def receive[A](msg: BinaryMessage)(ack: => A)(onExit: (Exit[Any, Any], ReplyCodec) => A): Task[A] =
    ZIO.attempt {
      val decoded = recipientType.codec.decodeMessage(msg.body)
      (decoded, recipientType.codec.replyCodec(decoded))
    }
      .mapError(Replies.serializationError("decode a message", _))
      .flatMap((decoded, codec) =>
        if (msg.expectsReply) ask(msg.entityId, decoded).map(onExit(_, codec))
        else enqueue(msg.entityId, decoded).as(ack)
      )

  /**
   * Add a message to the entity's mailbox, or fail with `MailboxFull` if it is full.
   * Returns None if the entity is closing.
   */
  private def tryOffer(entity: RunningEntity[Msg], message: Msg[Any, Any]): IO[MailboxFull, Option[Envelope[Msg]]] = {
    val mailbox = if (message.isInstanceOf[Concurrent]) entity.concurrentMailbox else entity.mailbox
    val add     = Envelope.make(message).tap(mailbox.offer).asSome
    // uninterruptible so that a message counted in `pending` always reaches the mailbox
    entity.state.modify { s =>
      // no need to update lastActive: the entity is not stopped while it has pending messages
      if (s.closing) (ZIO.none, s)
      else if (s.pending >= options.mailboxCapacity) (ZIO.fail(MailboxFull(recipientType.name, entity.entityId)), s)
      else (add, s.copy(pending = s.pending + 1))
    }.flatten.uninterruptible
  }

  private def getOrStart(entityId: String): IO[EntityNotManagedByThisPod, RunningEntity[Msg]] =
    entities.modifyZIO { map =>
      map.get(entityId) match {
        case Some(entity) => ZIO.succeed((entity, map))
        case None         =>
          // check again under the lock, so that an entity is never started after being stopped
          (isOnLocalShards(entityId) && sharding.isShuttingDown.negate && closed.get.negate).flatMap {
            case true  => start(entityId).map(entity => (entity, map.updated(entityId, entity)))
            case false => ZIO.fail(EntityNotManagedByThisPod(entityId))
          }
      }
    }.uninterruptible

  private def start(entityId: String): UIO[RunningEntity[Msg]] =
    for {
      mailbox    <- Queue.unbounded[Envelope[Msg]]
      concurrent <- Queue.unbounded[Envelope[Msg]]
      now        <- currentTimeInMilliseconds
      state      <- Ref.make(
                      RunningEntity.State(
                        pending = 0,
                        streams = 0,
                        closing = false,
                        lastActive = now
                      )
                    )
      drained    <- Promise.make[Nothing, Unit]
      closing    <- Promise.make[Nothing, Unit]
      terminated <- Promise.make[Nothing, Unit]
      entity      = new RunningEntity(entityId, mailbox, concurrent, state, drained, closing, terminated)
      // the entity runs with the fiber refs of the registration (e.g. log annotations), not those of the message that
      // started it, which may come from any sender
      // the gauge is updated in that fiber to keep it out of the lock of `entities`
      _          <-
        (ZIO.updateFiberRefs((fiberId, _) => registered.forkAs(fiberId)) *> gauge.increment *> run(entity)).forkDaemon
    } yield entity

  /**
   * Run an entity until it stops, wrapped by the interceptors.
   */
  private def run(entity: RunningEntity[Msg]): UIO[Unit] =
    // the fibers of the entity are forked inside the interceptors, so that they inherit what they set (e.g. log annotations)
    ZIO
      .suspendSucceed(
        // an entity given up on before the interceptor ran it must not start anymore
        interceptor.aroundEntity(recipientType, entity.entityId)(
          ZIO.unlessZIO(entity.terminated.isDone)(lifecycle(entity)).unit
        )
      )
      .catchAllCause(cause =>
        // its messages fail like with a behavior that fails (none are left if it died after the end of the entity)
        ZIO.logErrorCause(s"The interceptor of ${recipientType.name} ${entity.entityId} died", cause) *>
          failToStart(entity, cause) *> remove(entity)
      )
      .ensuring(entity.terminated.succeed(()))
      .uninterruptible

  /**
   * Start the handler of an entity, then stop it once the entity is closing.
   */
  private def lifecycle(entity: RunningEntity[Msg]): UIO[Unit] =
    for {
      scope      <- Scope.make
      // must stay interruptible to be stopped
      processing <- makeHandler(entity.entityId)
                      .provideEnvironment(ZEnvironment[Scope](scope))
                      .foldCauseZIO(
                        cause =>
                          ZIO.logErrorCause(s"Failed to start ${recipientType.name} ${entity.entityId}", cause) *>
                            failToStart(entity, cause),
                        handler => {
                          val mainLoop: UIO[Nothing] =
                            if (sequential) sequentialLoop(entity, handler)
                            else
                              ZIO
                                .when(options.concurrency <= maxBoundedConcurrency)(
                                  Queue.bounded[Unit](options.concurrency)
                                )
                                .flatMap(permits => consume(entity.mailbox)(dispatch(entity, handler, _, permits)))
                          mainLoop.fork *> consume(entity.concurrentMailbox)(dispatch(entity, handler, _, None))
                        }
                      )
                      .interruptible
                      .fork
      _          <- entity.closing.await
      // interruptible so that the timeout works
      drained    <- entity.drained.await.interruptible.timeout(config.entityTerminationTimeout).map(_.isDefined)
      _          <-
        ZIO.unless(drained)(
          ZIO.logWarning(
            s"${recipientType.name} ${entity.entityId} did not complete its messages within ${config.entityTerminationTimeout.render}, interrupting them"
          )
        )
      _          <- processing.interrupt *> finishQueued(entity, interrupted)
      _          <- scope
                      .close(Exit.unit)
                      .catchAllCause(ZIO.logErrorCause(s"A finalizer of ${recipientType.name} ${entity.entityId} died", _))
      // right after its scope is closed, without waiting for the interceptor
      _          <- remove(entity)
    } yield ()

  /**
   * Remove an entity from the running entities, unless it was already removed.
   */
  private def remove(entity: RunningEntity[Msg]): UIO[Unit] =
    // compared by reference, since an entity with the same ID may have started since
    entities.modify { map =>
      if (map.get(entity.entityId).contains(entity)) (gauge.decrement, map - entity.entityId) else (ZIO.unit, map)
    }.flatten

  /**
   * Stop an entity whose handler failed to start.
   * Every message already sent to it fails with that error.
   */
  private def failToStart(entity: RunningEntity[Msg], cause: Cause[Nothing]): UIO[Unit] =
    // the shutdown waits until these messages are finished
    (stopInBackground(entity) *> finishQueued(entity, Exit.failCause(cause))).uninterruptible

  private def consume(mailbox: Queue[Envelope[Msg]])(f: Envelope[Msg] => UIO[Any]): UIO[Nothing] =
    mailbox.take.interruptible.flatMap(f).uninterruptible.forever

  /**
   * Fork a child of the caller that is given its own fiber.
   */
  private def forkWithSelf[A](f: Fiber.Runtime[Nothing, Any] => UIO[A]): UIO[Fiber.Runtime[Nothing, A]] =
    Promise.make[Nothing, Fiber.Runtime[Nothing, Any]].flatMap(self => self.await.flatMap(f).fork.tap(self.succeed))

  /**
   * Run the handler on a message on the given fiber and pass its result to `onExit`, unless the message was cancelled.
   */
  private def runIfQueued(
    entity: RunningEntity[Msg],
    handler: EntityHandler[Msg],
    envelope: Envelope[Msg],
    fiber: Fiber.Runtime[Nothing, Any]
  )(onExit: Exit[Any, Any] => UIO[Unit]): UIO[Unit] =
    envelope.state.modify {
      case EnvelopeState.Queued =>
        (handle(entity, handler, envelope.message).interruptible.exit.flatMap(onExit), EnvelopeState.Running(fiber))
      // the sender stopped waiting before the message was processed
      case other                => (finish(entity, envelope, interrupted), other)
    }.flatten

  private def sequentialLoop(entity: RunningEntity[Msg], handler: EntityHandler[Msg]): UIO[Nothing] =
    // a cancelled message interrupts the loop's fiber, then a new one takes over
    // the loop is a child of the dispatcher, so that interrupting the dispatcher stops it
    forkWithSelf(loop =>
      consume(entity.mailbox)(envelope =>
        runIfQueued(entity, handler, envelope, loop)(exit =>
          envelope.state.getAndSet(EnvelopeState.Done).flatMap { previous =>
            complete(entity, envelope, exit) *>
              // cancelled while running: wait for the sender's interruption, then a new loop takes over
              ZIO.when(previous == EnvelopeState.Done)(ZIO.interruptible(ZIO.never)).unit
          }
        )
      )
    ).flatMap(_.await).forever

  private def dispatch(
    entity: RunningEntity[Msg],
    handler: EntityHandler[Msg],
    envelope: Envelope[Msg],
    permit: Option[Queue[Unit]]
  ): UIO[Any] = {
    val releasePermit = permit.fold[UIO[Any]](ZIO.unit)(_.take)
    permit.fold[UIO[Any]](ZIO.unit)(permits =>
      // the entity is stopping before the message started
      permits.offer(()).interruptible.onInterrupt(finish(entity, envelope, interrupted))
    ) *>
      // the forked fiber marks the message as running before calling the handler,
      // so that a message cancelled before that never starts
      // called from an uninterruptible region, so that the forked fiber starts uninterruptible
      // a child of the dispatcher, so that interrupting the dispatcher interrupts it and waits for it
      forkWithSelf(fiber =>
        runIfQueued(entity, handler, envelope, fiber)(complete(entity, envelope, _)) *> releasePermit
      )
  }

  /**
   * Run the handler on a message.
   * Once a message has died, the next ones are interrupted instead of processed.
   */
  private def handle(entity: RunningEntity[Msg], handler: EntityHandler[Msg], message: Msg[Any, Any]): IO[Any, Any] =
    // this message is still pending, so `drained` can only be done here if a message died
    entity.drained.isDone.flatMap(died =>
      if (died) interrupted
      else
        // a handler or interceptor throwing instead of returning an effect is a defect
        interceptor.aroundMessage(recipientType, entity.entityId, message)(ZIO.suspendSucceed(handler.handle(message)))
    )

  private def complete(entity: RunningEntity[Msg], envelope: Envelope[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    exit match {
      case Exit.Success(Streamed(stream))         =>
        finish(entity, envelope, Exit.succeed(StreamReply(usesInput = false, _ => track(entity, stream))))
      case Exit.Success(duplex: Duplex[in, ?, ?]) =>
        val run = duplex.run.asInstanceOf[ZStream[Any, Nothing, Any] => ZStream[Any, Any, Any]]
        // a function that throws is a defect of the stream
        finish(
          entity,
          envelope,
          Exit.succeed(StreamReply(usesInput = true, input => track(entity, ZStream.suspend(run(input)))))
        )
      case Exit.Failure(cause) if cause.isDie     =>
        // the state may be corrupt: stop the entity before replying, so that later messages start a new one
        // don't wait for the other messages, since the state may be corrupt
        entity.drained.succeed(()) *>
          ZIO.logErrorCause(
            s"${recipientType.name} ${entity.entityId} died while processing a message, stopping it",
            cause
          ) *>
          stopInBackground(entity) *> finish(entity, envelope, exit)
      case _                                      =>
        finish(entity, envelope, exit)
    }

  /**
   * Complete a message with its result.
   */
  private def finish(entity: RunningEntity[Msg], envelope: Envelope[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    envelope.result.succeed(exit) *> release(entity)

  /**
   * Keep the entity running while a stream it returned is open, and cancel the stream when the entity stops.
   * A stream started while the entity is stopping fails with `Sharding.Retry`.
   */
  private def track(entity: RunningEntity[Msg], stream: ZStream[Any, Any, Any]): ZStream[Any, Any, Any] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(
          entity.state.modify(s => if (s.closing) (false, s) else (true, s.copy(streams = s.streams + 1)))
        )(ZIO.when(_)(release(entity, stream = true)))
        .map(running =>
          if (running) stream.interruptWhen(entity.closing.await *> ZIO.fail(StreamCancelled))
          else ZStream.fail(Sharding.Retry(EntityNotManagedByThisPod(entity.entityId)))
        )
    )

  /**
   * Mark a message, or an open stream if `stream` is true, as done.
   */
  private def release(entity: RunningEntity[Msg], stream: Boolean = false): UIO[Unit] =
    currentTimeInMilliseconds.flatMap { now =>
      entity.state.modify { s =>
        val updated =
          if (stream) s.copy(streams = s.streams - 1, lastActive = now)
          else s.copy(pending = s.pending - 1, lastActive = now)
        (updated.closing && updated.isIdle, updated)
      }
    }.flatMap(ZIO.when(_)(entity.drained.succeed(()))).unit

  /**
   * Stop an entity without waiting, if `condition` holds.
   * The entity accepts no new messages once this returns, and is given up on if it doesn't terminate in time.
   */
  private def stopInBackground(
    entity: RunningEntity[Msg],
    condition: RunningEntity.State => Boolean = _ => true
  ): UIO[Unit] =
    entity.state.modify { s =>
      if (s.closing || !condition(s)) (ZIO.unit, s)
      else
        (
          entity.closing.succeed(()) *> ZIO.when(s.isIdle)(entity.drained.succeed(())) *> abandonIfStuck(entity),
          s.copy(closing = true)
        )
    }.flatten.uninterruptible

  /**
   * In the background, give up on an entity that hasn't terminated after twice `entityTerminationTimeout`, the longest
   * its stop should take: the interceptor, a handler or a finalizer may never return.
   * Its queued messages are completed once only, since a message that outlived the stop's timeout may never finish.
   */
  private def abandonIfStuck(entity: RunningEntity[Msg]): UIO[Unit] =
    entity.terminated.await.interruptible // interruptible so that the timeout works
      .timeout(config.entityTerminationTimeout * 2)
      .someOrElseZIO(
        ZIO.logWarning(
          s"${recipientType.name} ${entity.entityId} did not terminate within twice ${config.entityTerminationTimeout.render}, leaving it in the background"
        ) *> finishMailboxes(entity, interrupted) *>
          // before removing it, so that an interceptor running the entity late can't start it next to a new instance
          entity.terminated.succeed(()) *> remove(entity)
      )
      .forkDaemon
      .unit

  /**
   * Complete the messages still in the entity's mailboxes with the given result.
   * Call it once the entity is closing and nothing takes messages from its mailboxes anymore.
   */
  private def finishQueued(entity: RunningEntity[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    // messages accepted before closing may not be in the mailbox yet, hence the retry
    finishMailboxes(entity, exit) *>
      entity.state.get.flatMap(s => if (s.pending == 0) ZIO.unit else ZIO.yieldNow *> finishQueued(entity, exit))

  private def finishMailboxes(entity: RunningEntity[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    (entity.mailbox.takeAll <*> entity.concurrentMailbox.takeAll).flatMap { case (envelopes, concurrent) =>
      ZIO.foreachDiscard(envelopes ++ concurrent)(finish(entity, _, exit))
    }

  def terminateEntity(entityId: String): UIO[Unit] =
    // don't make the caller wait, since this can be called from the entity itself
    entities.get.flatMap(_.get(entityId) match {
      case Some(entity) => stopInBackground(entity)
      case None         => ZIO.unit
    })

  // reads the entities under the lock, so that no entity is being started at the same time
  private val runningEntities: UIO[Map[String, RunningEntity[Msg]]] =
    entities.modifyZIO(map => ZIO.succeed((map, map)))

  /**
   * Get the IDs of the entities running on this pod, from their start until their scope is closed,
   * or until twice `entityTerminationTimeout` after their stop began if they take longer.
   */
  def entityIds: UIO[Set[String]] =
    // under the lock, so that an entity whose behavior was called is always included
    runningEntities.map(_.keySet)

  def terminateEntitiesOnShards(shards: Set[ShardId]): UIO[Unit] =
    recipientType match {
      case _: EntityType[?] =>
        runningEntities.flatMap { map =>
          stopAll(map.collect {
            case (entityId, entity) if shards.contains(sharding.getShardId(recipientType, entityId)) => entity
          })
        }
      // each pod runs its own instance of a topic, whichever pod holds the shard of its name
      case _: TopicType[?]  => ZIO.unit
    }

  def terminateAllEntities: UIO[Unit] =
    runningEntities.flatMap(map => stopAll(map.values))

  /**
   * Stop all entities and never start new ones.
   * Used when the entity or topic type is unregistered.
   */
  def close: UIO[Unit] =
    closed.set(true) *> terminateAllEntities

  private def stopAll(running: Iterable[RunningEntity[Msg]]): UIO[Unit] =
    ZIO
      .foreachParDiscard(running)(entity => stopInBackground(entity) *> entity.terminated.await)
      .withParallelismUnbounded
}
