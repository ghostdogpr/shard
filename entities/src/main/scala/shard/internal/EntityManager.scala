package shard.internal

import shard._
import shard.errors.{ EntityNotManagedByThisPod, MailboxFull, MessageDiscarded, StreamCancelled }
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.Pods.BinaryMessage
import zio.{ Config => _, _ }
import zio.stream.ZStream

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private[shard] object EntityManager {

  /**
   * The reply of a Streamed or Duplex message: runs its stream given its inputs (ignored for Streamed).
   */
  final case class StreamReply(run: ZStream[Any, Nothing, Any] => ZStream[Any, Any, Any])

  private sealed trait EnvelopeState
  private object EnvelopeState {
    case object Queued                                           extends EnvelopeState
    case object Cancelled                                        extends EnvelopeState
    final case class Running(fiber: Fiber.Runtime[Nothing, Any]) extends EnvelopeState
    // completed, or cancelled by the sender while it was running: its fiber is being interrupted
    case object Done                                             extends EnvelopeState
  }

  /**
   * A message enqueued for an entity.
   */
  final class Envelope[Msg[+_, +_]] private[EntityManager] (
    val message: Msg[Any, Any],
    private[EntityManager] val result: Promise[Nothing, Exit[Any, Any]],
    private[EntityManager] val state: Ref[EnvelopeState]
  ) {

    // Streamed and Duplex replies are returned as a StreamReply
    def await: UIO[Exit[Any, Any]] = result.await

    // called when the sender stops waiting
    def cancel: UIO[Unit] =
      if (message.isInstanceOf[Uninterruptible]) ZIO.unit
      else
        state.modify {
          case EnvelopeState.Queued         => (ZIO.unit, EnvelopeState.Cancelled)
          case EnvelopeState.Running(fiber) => (fiber.interruptFork, EnvelopeState.Done)
          case other                        => (ZIO.unit, other)
        }.flatten
  }

  private object Envelope {
    def make[Msg[+_, +_]](message: Msg[Any, Any]): UIO[Envelope[Msg]] =
      for {
        result <- Promise.make[Nothing, Exit[Any, Any]]
        state  <- Ref.make[EnvelopeState](EnvelopeState.Queued)
      } yield new Envelope(message, result, state)
  }

  /**
   * @param drained completed when the entity is closing and has no pending messages or open streams
   * @param closing completed when the entity starts closing, to interrupt its streams
   * @param poisoned set when processing a message died: the next messages are not processed
   */
  private final class RunningEntity[Msg[+_, +_]](
    val entityId: String,
    val mailbox: Queue[Envelope[Msg]],
    val concurrentMailbox: Queue[Envelope[Msg]],
    val state: Ref[RunningEntity.State],
    val drained: Promise[Nothing, Unit],
    val closing: Promise[Nothing, Unit],
    val terminated: Promise[Nothing, Unit],
    val poisoned: AtomicBoolean,
    val scope: Scope.Closeable,
    val dispatcher: Promise[Nothing, Fiber.Runtime[Nothing, Any]]
  )

  private object RunningEntity {

    /**
     * @param pending number of messages enqueued and not yet completed, limited by `mailboxCapacity`
     * @param streams number of open reply streams, which don't count in `pending` but keep the entity running
     * @param offering number of messages accepted but not yet in the mailbox
     */
    final case class State(
      pending: Int,
      streams: Int,
      offering: Int,
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

  def make[Msg[+_, +_]](
    recipientType: RecipientType[Msg],
    makeHandler: String => URIO[Scope, EntityHandler[Msg]],
    sharding: Sharding,
    config: Config,
    options: EntityOptions[Msg]
  ): URIO[Scope, EntityManager[Msg]] =
    for {
      entities <- Ref.Synchronized.make[Map[String, RunningEntity[Msg]]](Map())
      closed   <- Ref.make(false)
      manager   = new EntityManager(recipientType, makeHandler, entities, closed, sharding, config, options)
      _        <- manager.reaper.interruptible.forkScoped
    } yield manager
}

/**
 * Runs the entities of a given entity or topic type on this pod.
 * Bookkeeping is done in uninterruptible regions so that an interruption never loses a message or leaves it
 * uncompleted. Only the processing of the message itself is interruptible.
 */
private[shard] final class EntityManager[Msg[+_, +_]] private (
  val recipientType: RecipientType[Msg],
  makeHandler: String => URIO[Scope, EntityHandler[Msg]],
  entities: Ref.Synchronized[Map[String, EntityManager.RunningEntity[Msg]]],
  closed: Ref[Boolean],
  sharding: Sharding,
  config: Config,
  options: EntityOptions[Msg]
) {
  import EntityManager._

  private val gauge = Metrics.entities.tagged("role", config.role.name).tagged("type", recipientType.name)

  private val interrupted: Exit[Any, Any] = Exit.interrupt(FiberId.None)

  // messages processed one at a time run in the fiber of a loop, replaced when the sender cancels a message
  private val sequential = options.concurrency <= 1

  // stops entities that have been idle for longer than maxIdleTime
  val reaper: UIO[Nothing] = {
    val maxIdleTime   = options.maxIdleTime getOrElse config.entityMaxIdleTime
    val maxIdleMillis = maxIdleTime.toMillis
    val interval      = (maxIdleTime dividedBy 10L) max 10.millis min 5.seconds
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
   * Enqueues a message for the given entity, starting the entity if needed.
   * Fails with `EntityNotManagedByThisPod`, `MailboxFull` or `MessageDiscarded`.
   */
  def enqueue(entityId: String, message: Msg[Any, Any]): IO[Throwable, Envelope[Msg]] =
    for {
      _        <- ZIO.unlessZIO(isOnLocalShards(entityId))(ZIO.fail(EntityNotManagedByThisPod(entityId)))
      map      <- entities.get
      envelope <- map.get(entityId) match {
                    case Some(entity)                         => offer(entity, message)
                    case None if !options.loadEntity(message) =>
                      ZIO.fail(MessageDiscarded(recipientType.name, entityId))
                    case None                                 => getOrStart(entityId).flatMap(offer(_, message))
                  }
    } yield envelope

  def enqueueBinary(msg: BinaryMessage): Task[(Envelope[Msg], ReplyCodec)] =
    ZIO
      .attempt(recipientType.codec.decodeMessage(msg.body))
      .flatMap(decoded => enqueue(msg.entityId, decoded).map(_ -> recipientType.codec.replyCodec(decoded)))

  private def offer(entity: RunningEntity[Msg], message: Msg[Any, Any]): IO[Throwable, Envelope[Msg]] =
    // the entity is stopping, try again a little later
    tryOffer(entity, message).someOrElseZIO(Clock.sleep(100.millis) *> enqueue(entity.entityId, message))

  // returns None if the entity is closing
  // uninterruptible so that `offering` always goes back down once the message is in the mailbox
  private def tryOffer(entity: RunningEntity[Msg], message: Msg[Any, Any]): IO[MailboxFull, Option[Envelope[Msg]]] = {
    val mailbox = if (message.isInstanceOf[Concurrent]) entity.concurrentMailbox else entity.mailbox
    val add     = Envelope
      .make(message)
      .tap(mailbox.offer)
      .flatMap(envelope => entity.state.update(s => s.copy(offering = s.offering - 1)).as(Some(envelope)))
    entity.state.modify { s =>
      // no need to update lastActive: the entity is not stopped while it has pending messages
      if (s.closing) (ZIO.none, s)
      else if (s.pending >= options.mailboxCapacity) (ZIO.fail(MailboxFull(recipientType.name, entity.entityId)), s)
      else (add, s.copy(pending = s.pending + 1, offering = s.offering + 1))
    }.flatten.uninterruptible
  }

  private def getOrStart(entityId: String): IO[EntityNotManagedByThisPod, RunningEntity[Msg]] =
    entities.modifyZIO { map =>
      map.get(entityId) match {
        case Some(entity) => ZIO.succeed((entity, map))
        case None         =>
          // check again under the lock, so that an entity is never started after unregister/unassign/close stopped them
          (sharding.isShuttingDown <*> isOnLocalShards(entityId) <*> closed.get).flatMap {
            case (false, true, false) => start(entityId).map(entity => (entity, map.updated(entityId, entity)))
            case _                    => ZIO.fail(EntityNotManagedByThisPod(entityId))
          }
      }
    }.uninterruptible

  private def start(entityId: String): UIO[RunningEntity[Msg]] =
    for {
      mailbox    <- Queue.unbounded[Envelope[Msg]]
      concurrent <- Queue.unbounded[Envelope[Msg]]
      now        <- currentTimeInMilliseconds
      state      <- Ref.make(RunningEntity.State(pending = 0, streams = 0, offering = 0, closing = false, lastActive = now))
      drained    <- Promise.make[Nothing, Unit]
      closing    <- Promise.make[Nothing, Unit]
      terminated <- Promise.make[Nothing, Unit]
      scope      <- Scope.make
      dispatcher <- Promise.make[Nothing, Fiber.Runtime[Nothing, Any]]
      entity      = new RunningEntity(
                      entityId,
                      mailbox,
                      concurrent,
                      state,
                      drained,
                      closing,
                      terminated,
                      new AtomicBoolean(false),
                      scope,
                      dispatcher
                    )
      _          <- gauge.increment
      // must stay interruptible to be stopped
      fiber      <- run(entity).interruptible.forkDaemon
      _          <- dispatcher.succeed(fiber)
    } yield entity

  private def run(entity: RunningEntity[Msg]): UIO[Any] =
    makeHandler(entity.entityId)
      .provideEnvironment(ZEnvironment[Scope](entity.scope))
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
                .flatMap(dispatcherLoop(entity, handler, entity.mailbox, _))
          mainLoop <&> dispatcherLoop(entity, handler, entity.concurrentMailbox, None)
        }
      )

  // every message enqueued so far receives the error as a defect
  private def failToStart(entity: RunningEntity[Msg], cause: Cause[Nothing]): UIO[Unit] =
    // the shutdown waits until these messages are finished
    (stopInBackground(entity) *> finishQueued(entity, Exit.failCause(cause))).uninterruptible

  // when a cancelled message interrupts the loop's fiber, a new one takes over
  // it is a child of the dispatcher, so that interrupting the dispatcher stops it
  private def sequentialLoop(entity: RunningEntity[Msg], handler: EntityHandler[Msg]): UIO[Nothing] =
    Promise
      .make[Nothing, Fiber.Runtime[Nothing, Any]]
      .flatMap { self =>
        self.await
          .flatMap(fiber =>
            ZIO
              .uninterruptibleMask(restore =>
                restore(entity.mailbox.take).flatMap(process(entity, handler, _, fiber, restore))
              )
              .forever
          )
          .fork
          .tap(self.succeed)
          .flatMap(_.await)
      }
      .forever

  // must be called from an uninterruptible region
  private def process(
    entity: RunningEntity[Msg],
    handler: EntityHandler[Msg],
    envelope: Envelope[Msg],
    loop: Fiber.Runtime[Nothing, Any],
    restore: ZIO.InterruptibilityRestorer
  ): UIO[Unit] =
    if (entity.poisoned.get) finish(entity, envelope, interrupted)
    else
      envelope.state.modify {
        case EnvelopeState.Queued => (true, EnvelopeState.Running(loop))
        case other                => (false, other)
      }.flatMap {
        case false =>
          // the sender stopped waiting before the message was processed
          finish(entity, envelope, interrupted)
        case true  =>
          restore(handle(handler, envelope.message)).exit
            .flatMap(exit =>
              envelope.state.modify {
                case EnvelopeState.Running(_) => (false, EnvelopeState.Done)
                case _                        => (true, EnvelopeState.Done) // cancelled while running
              }.flatMap(cancelled =>
                complete(entity, envelope, exit) *>
                  // the sender interrupted this fiber: wait for the interruption, then a new loop takes over
                  ZIO.when(cancelled)(ZIO.interruptible(ZIO.never)).unit
              )
            )
      }

  private def dispatcherLoop(
    entity: RunningEntity[Msg],
    handler: EntityHandler[Msg],
    mailbox: Queue[Envelope[Msg]],
    permits: Option[Queue[Unit]]
  ): UIO[Nothing] =
    ZIO
      .uninterruptibleMask(restore => restore(mailbox.take).flatMap(dispatch(entity, handler, _, permits, restore)))
      .forever

  // must be called from an uninterruptible region
  private def dispatch(
    entity: RunningEntity[Msg],
    handler: EntityHandler[Msg],
    envelope: Envelope[Msg],
    permit: Option[Queue[Unit]],
    restore: ZIO.InterruptibilityRestorer
  ): UIO[Unit] =
    if (entity.poisoned.get) finish(entity, envelope, interrupted)
    else
      envelope.state.get.flatMap {
        case EnvelopeState.Cancelled =>
          // the sender stopped waiting before the message was processed
          finish(entity, envelope, interrupted)
        case _                       =>
          permit.fold[UIO[Any]](ZIO.unit)(permits =>
            // the entity is stopping before the message started
            restore(permits.offer(())).onInterrupt(finish(entity, envelope, interrupted))
          ) *> fork(entity, envelope, handle(handler, envelope.message), permit)
      }

  // must be called from an uninterruptible region, so that the fiber starts uninterruptible
  private def fork(
    entity: RunningEntity[Msg],
    envelope: Envelope[Msg],
    effect: IO[Any, Any],
    permit: Option[Queue[Unit]]
  ): UIO[Unit] = {
    val body =
      effect.interruptible.exit.flatMap(exit =>
        permit.fold[UIO[Any]](ZIO.unit)(_.take) *>
          complete(entity, envelope, exit)
      )
    // a child of the dispatcher, so that interrupting the dispatcher interrupts it and waits for it
    body.fork.flatMap(fiber =>
      envelope.state.modify {
        case EnvelopeState.Cancelled => (fiber.interruptFork, EnvelopeState.Done)
        case _                       => (ZIO.unit, EnvelopeState.Running(fiber))
      }.flatten
    )
  }

  // a handler throwing instead of returning an effect is a defect
  private def handle(handler: EntityHandler[Msg], message: Msg[Any, Any]): IO[Any, Any] =
    ZIO.suspendSucceed(handler.handle(message))

  private def complete(entity: RunningEntity[Msg], envelope: Envelope[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    exit match {
      case Exit.Success(Streamed(stream))         =>
        finish(entity, envelope, Exit.succeed(StreamReply(_ => track(entity, stream))))
      case Exit.Success(duplex: Duplex[in, ?, ?]) =>
        val run = duplex.run.asInstanceOf[ZStream[Any, Nothing, Any] => ZStream[Any, Any, Any]]
        finish(entity, envelope, Exit.succeed(StreamReply(input => track(entity, run(input)))))
      case Exit.Failure(cause) if cause.isDie     =>
        // its state may be corrupt: don't process the next messages, and stop the entity before replying so that
        // the messages sent after the reply start a new one
        ZIO.succeed(entity.poisoned.set(true)) *>
          ZIO.logErrorCause(
            s"${recipientType.name} ${entity.entityId} died while processing a message, stopping it",
            cause
          ) *> stopInBackground(entity) *> finish(entity, envelope, exit)
      case _                                      =>
        finish(entity, envelope, exit)
    }

  // does nothing if the message was already completed
  private def finish(entity: RunningEntity[Msg], envelope: Envelope[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    envelope.result.succeed(exit).flatMap(ZIO.when(_)(release(entity))).unit

  /**
   * Keeps the entity running while the stream is open, and cancels it when the entity stops.
   * A stream started once the entity is stopping fails with `Sharding.Retry` so that the sender sends the message again.
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

  // marks a message, or an open stream if `stream` is true, as done
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
   * Stops an entity without waiting, if `condition` holds. It doesn't accept new messages once this returns.
   * The shutdown runs in its own fiber so that interrupting the caller doesn't leave it half done.
   */
  private def stopInBackground(
    entity: RunningEntity[Msg],
    condition: RunningEntity.State => Boolean = _ => true
  ): UIO[Unit] =
    entity.state
      .modify(s => if (s.closing || !condition(s)) (false, s) else (true, s.copy(closing = true)))
      .flatMap(ZIO.when(_)(shutdown(entity).forkDaemon))
      .unit
      .uninterruptible

  // must be called once the entity is marked as closing
  private def shutdown(entity: RunningEntity[Msg]): UIO[Unit] =
    (for {
      _          <- entity.closing.succeed(())
      idle       <- entity.state.get.map(_.isIdle)
      _          <- ZIO.when(idle)(entity.drained.succeed(()))
      // interruptible so that the timeout works; don't wait if a message died since the state may be corrupt
      drained    <- if (entity.poisoned.get) ZIO.succeed(true)
                    else
                      entity.drained.await.interruptible.timeout(config.entityTerminationTimeout).map(_.isDefined)
      _          <-
        ZIO.unless(drained)(
          ZIO.logWarning(
            s"${recipientType.name} ${entity.entityId} did not complete its messages within ${config.entityTerminationTimeout.render}, interrupting them"
          )
        )
      dispatcher <- entity.dispatcher.await
      _          <- dispatcher.interrupt
      _          <- finishQueued(entity, interrupted)
      _          <- entity.scope
                      .close(Exit.unit)
                      .catchAllCause(
                        ZIO.logErrorCause(s"A finalizer of ${recipientType.name} ${entity.entityId} died", _)
                      )
      _          <- entities.update(map => if (map.get(entity.entityId).exists(_ eq entity)) map - entity.entityId else map)
      _          <- gauge.decrement
    } yield ()).ensuring(entity.terminated.succeed(())).uninterruptible

  // completes the messages still in the mailboxes, once the entity is closing
  private def finishQueued(entity: RunningEntity[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    // messages accepted before closing may not be in the mailbox yet
    awaitOffers(entity) *> (entity.mailbox.takeAll <*> entity.concurrentMailbox.takeAll).flatMap {
      case (envelopes, concurrent) => ZIO.foreachDiscard(envelopes ++ concurrent)(finish(entity, _, exit))
    }

  // once the entity is closing, offering doesn't block, so this only waits for a few instructions
  private def awaitOffers(entity: RunningEntity[Msg]): UIO[Unit] =
    entity.state.get.flatMap(s => if (s.offering == 0) ZIO.unit else ZIO.yieldNow *> awaitOffers(entity))

  def terminateEntity(entityId: String): UIO[Unit] =
    // don't make the caller wait, since this can be called from the entity itself
    entities.get.flatMap(_.get(entityId) match {
      case Some(entity) => stopInBackground(entity)
      case None         => ZIO.unit
    })

  // reads the entities under the lock, so that no entity is being started at the same time
  private val runningEntities: UIO[Map[String, RunningEntity[Msg]]] =
    entities.modifyZIO(map => ZIO.succeed((map, map)))

  def terminateEntitiesOnShards(shards: Set[ShardId]): UIO[Unit] =
    runningEntities.flatMap { map =>
      stopAll(map.collect {
        case (entityId, entity) if shards.contains(sharding.getShardId(recipientType, entityId)) => entity
      })
    }

  def terminateAllEntities: UIO[Unit] =
    runningEntities.flatMap(map => stopAll(map.values))

  // when the entity or topic type is unregistered: no entity is started afterwards
  def close: UIO[Unit] =
    closed.set(true) *> terminateAllEntities

  private def stopAll(running: Iterable[RunningEntity[Msg]]): UIO[Unit] =
    ZIO
      .foreachParDiscard(running)(entity => stopInBackground(entity) *> entity.terminated.await)
      .withParallelismUnbounded
}
