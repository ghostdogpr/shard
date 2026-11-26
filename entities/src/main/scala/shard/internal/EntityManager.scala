package shard.internal

import java.util.concurrent.TimeUnit

import scala.collection.immutable

import zio.{Config => _, _}
import zio.stream.ZStream

import shard._
import shard.errors.{EntityNotManagedByThisPod, MailboxFull, MessageDiscarded, StreamCancelled}
import shard.interfaces.MessageCodec.ReplyCodec
import shard.interfaces.Pods.BinaryMessage

private[shard] object EntityManager {

  /**
    * The reply of a Streamed or Duplex message: runs its stream given its inputs (ignored for Streamed).
    */
  final case class StreamReply(usesInput: Boolean, run: ZStream[Any, Nothing, Any] => ZStream[Any, Any, Any])

  sealed private trait EnvelopeState
  private object EnvelopeState {
    case object Queued                                           extends EnvelopeState
    final case class Running(fiber: Fiber.Runtime[Nothing, Any]) extends EnvelopeState
    // cancelled by the sender
    case object Done                                             extends EnvelopeState
  }

  /**
    * What the fiber of an entity reacts to, in the order it was added to the entity's mailbox.
    */
  sealed trait Event[Msg[_, _]]
  private object Event {
    final case class Started[Msg[_, _]](handler: EntityHandler[Msg], fiberRefs: FiberRefs) extends Event[Msg]
    final case class FailedToStart[Msg[_, _]](cause: Cause[Nothing])                       extends Event[Msg]
    // `limited` if the message counted in the entity's concurrency
    final case class Done[Msg[_, _]](fiber: Fiber.Runtime[Nothing, Any], limited: Boolean) extends Event[Msg]
    final case class Finish[Msg[_, _]](drained: Boolean)                                   extends Event[Msg]
  }

  /**
    * A message enqueued for an entity.
    */
  final class Envelope[Msg[_, _]] private[EntityManager] (
    val message: Msg[Any, Any],
    private[EntityManager] val result: Promise[Nothing, Exit[Any, Any]],
    private[EntityManager] val state: Ref[EnvelopeState]
  ) extends Event[Msg] {

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
  final private class RunningEntity[Msg[_, _]](
    val entityId: String,
    val mailbox: Queue[Event[Msg]],
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

  final private case class Entities[Msg[_, _]](running: Map[String, RunningEntity[Msg]], collections: Long)

  private enum Phase[Msg[_, _]] {
    case Starting()
    case Ready(handler: EntityHandler[Msg])
    case FailedToStart(cause: Cause[Nothing])
  }

  /**
    * What the fiber of an entity keeps track of between two events.
    *
    * @param waiting messages received before the handler started, or beyond the entity's concurrency
    * @param limited number of running messages that count in the entity's concurrency
    * @param running fibers of the running messages
    * @param starter the fiber that creates the handler, while it runs
    */
  final private case class Processing[Msg[_, _]](
    phase: Phase[Msg],
    waiting: immutable.Queue[Envelope[Msg]],
    limited: Int,
    running: Set[Fiber.Runtime[Nothing, Any]],
    starter: Option[Fiber.Runtime[Nothing, Any]]
  )

  /**
    * @param fiber the fiber of the entity, which owns the fibers forked by the behavior and the handler, through `graft`
    */
  final private case class Lifecycle(scope: Scope.Closeable, fiber: Fiber.Runtime[Nothing, Any], graft: ZIO.Grafter)

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
      entities   <- Ref.make(Entities[Msg](Map(), 0L))
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
final private[shard] class EntityManager[Msg[_, _]] private (
  val recipientType: RecipientType[Msg],
  makeHandler: String => URIO[Scope, EntityHandler[Msg]],
  entities: Ref[EntityManager.Entities[Msg]],
  closed: Ref[Boolean],
  registered: FiberRefs,
  sharding: Sharding,
  config: Config,
  options: EntityOptions[Msg]
) {
  import EntityManager._

  private val gauge = Metrics.entities.tagged("role", config.role.name).tagged("type", recipientType.name)

  private val decrementGauge = FiberRef.currentTags.locally(registered.getOrDefault(FiberRef.currentTags))(gauge.decrement)

  private val interrupted: Exit[Any, Any] = Exit.interrupt(FiberId.None)

  private val concurrency = options.concurrency.max(1)

  private val interceptor = config.entityInterceptor ++ options.interceptor

  val reaper: UIO[Nothing] = {
    val maxIdleTime   = options.maxIdleTime getOrElse config.entityMaxIdleTime
    val maxIdleMillis = maxIdleTime.toMillis
    val interval      = maxIdleTime.dividedBy(10L).max(10.millis).min(5.seconds)
    val reap          =
      for {
        now     <- currentTimeInMilliseconds
        current <- entities.get
        _       <- ZIO.foreachDiscard(current.running.values)(
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
      current  <- entities.get
      envelope <- current.running.get(entityId) match {
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
    ZIO
      .attempt {
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
    val add = Envelope.make(message).tap(entity.mailbox.offer).asSome
    // uninterruptible so that a message counted in `pending` always reaches the mailbox
    entity.state
      .modify { s =>
        // no need to update lastActive: the entity is not stopped while it has pending messages
        if (s.closing) (ZIO.none, s)
        else if (s.pending >= options.mailboxCapacity) (ZIO.fail(MailboxFull(recipientType.name, entity.entityId)), s)
        else (add, s.copy(pending = s.pending + 1))
      }
      .flatten
      .uninterruptible
  }

  private def getOrStart(entityId: String): IO[EntityNotManagedByThisPod, RunningEntity[Msg]] =
    entities.get.flatMap { before =>
      before.running.get(entityId) match {
        case Some(entity) => ZIO.succeed(entity)
        case None         =>
          // check again, so that an entity is never started after being stopped
          (isOnLocalShards(entityId) && sharding.isShuttingDown.negate && closed.get.negate).flatMap {
            case true  =>
              create(entityId).flatMap { entity =>
                entities.modify { current =>
                  current.running.get(entityId) match {
                    case Some(other)                                       => (ZIO.succeed(other), current)
                    case None if current.collections != before.collections => (getOrStart(entityId), current)
                    // added before it runs, since an entity that fails to start removes itself right away
                    case None                                              =>
                      (start(entity).as(entity), current.copy(running = current.running.updated(entityId, entity)))
                  }
                }.flatten
              }
            case false => ZIO.fail(EntityNotManagedByThisPod(entityId))
          }
      }
    }.uninterruptible

  private def create(entityId: String): UIO[RunningEntity[Msg]] =
    for {
      mailbox    <- Queue.unbounded[Event[Msg]]
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
    } yield new RunningEntity(entityId, mailbox, state, drained, closing, terminated)

  private def start(entity: RunningEntity[Msg]): UIO[Unit] =
    forkWithSelf(fiber => withRegistration(gauge.increment *> run(entity, fiber))).unit

  // with the fiber refs of the registration (e.g. log annotations), not those of the caller, which may be any sender
  private def withRegistration[A](effect: UIO[A]): UIO[A] =
    ZIO.updateFiberRefs((fiberId, _) => registered.forkAs(fiberId)) *> effect

  /**
    * Run an entity until it stops, wrapped by the interceptors.
    */
  private def run(entity: RunningEntity[Msg], fiber: Fiber.Runtime[Nothing, Any]): UIO[Unit] =
    // the fibers of the entity are forked inside the interceptors, so that they inherit what they set (e.g. log annotations)
    ZIO
      .suspendSucceed(
        // skipped if the entity was stopped before the interceptor ran this effect
        interceptor.aroundEntity(recipientType, entity.entityId)(
          // uninterruptible even if the interceptor makes it interruptible: only messages can be interrupted
          ZIO.unlessZIO(entity.terminated.isDone)(lifecycle(entity, fiber)).unit.uninterruptible
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
    * Start the handler of an entity, process its messages, then stop it once it is told to finish.
    * Each message runs in its own fiber, so that the entity has a single fiber while it waits.
    */
  private def lifecycle(entity: RunningEntity[Msg], fiber: Fiber.Runtime[Nothing, Any]): UIO[Unit] =
    // nothing may follow `process`: ZIO would keep its first `Processing`, with the starter fiber, on the stack until the
    // entity stops, so `stop` does the cleanup instead
    ZIO.transplant(graft =>
      Scope.make.flatMap { scope =>
        // in its own fiber, so that a handler slow to start can be interrupted when the entity stops
        forkWithSelf(_ =>
          graft(makeHandler(entity.entityId).provideEnvironment(ZEnvironment[Scope](scope))).interruptible.exit
            .flatMap(exit =>
              ZIO.getFiberRefs.flatMap(fiberRefs => entity.mailbox.offer(exit.foldExit(Event.FailedToStart(_), Event.Started(_, fiberRefs))))
            )
        ).flatMap(starter =>
          process(
            entity,
            Lifecycle(scope, fiber, graft),
            Processing(
              phase = Phase.Starting(),
              waiting = immutable.Queue.empty,
              limited = 0,
              running = Set.empty,
              starter = Some(starter)
            )
          )
        )
      }
    )

  /**
    * Interrupt the messages still running and the fibers forked by the behavior and the handler,
    * complete the other messages with the same result, then close the entity's scope.
    */
  private def stop(entity: RunningEntity[Msg], lifecycle: Lifecycle, processing: Processing[Msg]): UIO[Unit] =
    lifecycle.fiber.children.flatMap { forked =>
      val fibers = processing.running ++ processing.starter ++ forked
      // nested, so that each step frees what the previous one held, even if a later step never returns
      ZIO.foreachDiscard(fibers)(_.interruptFork) *>
        (ZIO.foreachDiscard(processing.waiting)(finish(entity, _, interrupted)) *>
          (ZIO.foreachDiscard(fibers)(_.await) *>
            (finishQueued(entity, interrupted) *>
              (lifecycle.scope
                .close(Exit.unit)
                .catchAllCause(ZIO.logErrorCause(s"A finalizer of ${recipientType.name} ${entity.entityId} died", _)) *>
                // right after its scope is closed, without waiting for the interceptor
                remove(entity)))))
    }

  /**
    * React to the events of an entity until it is told to finish.
    */
  private def process(entity: RunningEntity[Msg], lifecycle: Lifecycle, processing: Processing[Msg]): UIO[Unit] =
    entity.mailbox.take.flatMap {
      case envelope: Envelope[Msg]           =>
        processing.phase match {
          case Phase.Starting()           =>
            process(entity, lifecycle, processing.copy(waiting = processing.waiting.enqueue(envelope)))
          case Phase.Ready(handler)       => dispatch(entity, lifecycle, handler, processing, envelope).flatMap(process(entity, lifecycle, _))
          case Phase.FailedToStart(cause) =>
            finish(entity, envelope, Exit.failCause(cause)) *> process(entity, lifecycle, processing)
        }
      case Event.Started(handler, fiberRefs) =>
        // messages see the fiber refs the behavior set (e.g. `logAnnotateScoped`)
        ZIO.inheritFiberRefs(fiberRefs) *>
          ZIO
            .foldLeft(processing.waiting)(
              processing.copy(phase = Phase.Ready(handler), waiting = immutable.Queue.empty, starter = None)
            )(dispatch(entity, lifecycle, handler, _, _))
            .flatMap(process(entity, lifecycle, _))
      case Event.FailedToStart(cause)        =>
        ZIO.logErrorCause(s"Failed to start ${recipientType.name} ${entity.entityId}", cause) *>
          stopInBackground(entity) *>
          // the messages received until the entity finishes fail with the same error
          ZIO.foreachDiscard(processing.waiting)(finish(entity, _, Exit.failCause(cause))) *>
          process(
            entity,
            lifecycle,
            processing.copy(
              phase = Phase.FailedToStart(cause),
              waiting = immutable.Queue.empty,
              starter = None
            )
          )
      case Event.Done(fiber, limited)        =>
        val done = processing.copy(
          limited = if (limited) processing.limited - 1 else processing.limited,
          running = processing.running - fiber
        )
        (done.phase, if (limited) done.waiting.dequeueOption else None) match {
          case (Phase.Ready(handler), Some((next, waiting))) =>
            dispatch(entity, lifecycle, handler, done.copy(waiting = waiting), next).flatMap(process(entity, lifecycle, _))
          case _                                             => process(entity, lifecycle, done)
        }
      case Event.Finish(drained)             =>
        ZIO
          .unless(drained)(
            ZIO.logWarning(
              s"${recipientType.name} ${entity.entityId} did not complete its messages within ${config.entityTerminationTimeout.render}, interrupting them"
            )
          )
          *> stop(entity, lifecycle, processing)
    }

  /**
    * Remove an entity from the running entities, unless it was already removed.
    */
  private def remove(entity: RunningEntity[Msg]): UIO[Unit] =
    // compared by reference, since an entity with the same ID may have started since
    entities.modify { current =>
      if (current.running.get(entity.entityId).contains(entity))
        (decrementGauge, current.copy(running = current.running - entity.entityId))
      else (ZIO.unit, current)
    }.flatten

  /**
    * Stop an entity whose interceptor died.
    * Every message already sent to it fails with that error.
    */
  private def failToStart(entity: RunningEntity[Msg], cause: Cause[Nothing]): UIO[Unit] =
    // the shutdown waits until these messages are finished
    (stopInBackground(entity) *> finishQueued(entity, Exit.failCause(cause))).uninterruptible

  // daemon: the entity tracks these fibers itself, and as children of its fiber they would stay in its children set
  // until a GC
  private def forkWithSelf[A](f: Fiber.Runtime[Nothing, Any] => UIO[A]): UIO[Fiber.Runtime[Nothing, A]] =
    Promise.make[Nothing, Fiber.Runtime[Nothing, Any]].flatMap(self => self.await.flatMap(f).forkDaemon.tap(self.succeed))

  private def runIfQueued(
    entity: RunningEntity[Msg],
    lifecycle: Lifecycle,
    handler: EntityHandler[Msg],
    envelope: Envelope[Msg],
    fiber: Fiber.Runtime[Nothing, Any]
  )(onExit: Exit[Any, Any] => UIO[Unit]): UIO[Unit] =
    envelope.state.modify {
      case EnvelopeState.Queued =>
        (
          lifecycle.graft(handle(entity, handler, envelope.message)).interruptible.exit.flatMap(onExit),
          EnvelopeState.Running(fiber)
        )
      // the sender stopped waiting before the message was processed
      case other                => (finish(entity, envelope, interrupted), other)
    }.flatten

  /**
    * Start a message in its own fiber, unless the entity's concurrency is reached: then it waits.
    * A `Concurrent` message always starts.
    */
  private def dispatch(
    entity: RunningEntity[Msg],
    lifecycle: Lifecycle,
    handler: EntityHandler[Msg],
    processing: Processing[Msg],
    envelope: Envelope[Msg]
  ): UIO[Processing[Msg]] = {
    val limited = !envelope.message.isInstanceOf[Concurrent]
    if (limited && processing.limited >= concurrency)
      ZIO.succeed(processing.copy(waiting = processing.waiting.enqueue(envelope)))
    else
      // starts uninterruptible and marks the message as running before calling the handler,
      // so that a message cancelled before that never starts
      forkWithSelf(fiber =>
        runIfQueued(entity, lifecycle, handler, envelope, fiber)(complete(entity, envelope, _)) *>
          entity.mailbox.offer(Event.Done(fiber, limited))
      ).map(fiber =>
        processing.copy(
          limited = if (limited) processing.limited + 1 else processing.limited,
          running = processing.running + fiber
        )
      )
  }

  /**
    * Run the handler on a message.
    * Once a message has died, the next ones are interrupted instead of processed.
    */
  private def handle(entity: RunningEntity[Msg], handler: EntityHandler[Msg], message: Msg[Any, Any]): IO[Any, Any] =
    // `drained` can only be done here if a message died, since this one hasn't finished
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
        // the state may be corrupt: stop the entity before replying, so that later messages start a new one,
        // without waiting for the other messages
        entity.drained.succeed(()) *>
          ZIO.logErrorCause(
            s"${recipientType.name} ${entity.entityId} died while processing a message, stopping it",
            cause
          ) *>
          stopInBackground(entity) *> finish(entity, envelope, exit)
      case _                                      =>
        finish(entity, envelope, exit)
    }

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
          if (running) Streams.interruptWhen(stream, entity.closing.await *> ZIO.fail(StreamCancelled))
          else ZStream.fail(Sharding.Retry(EntityNotManagedByThisPod(entity.entityId)))
        )
    )

  /**
    * Mark a message, or an open stream if `stream` is true, as done.
    */
  private def release(entity: RunningEntity[Msg], stream: Boolean = false): UIO[Unit] =
    currentTimeInMilliseconds
      .flatMap { now =>
        entity.state.modify { s =>
          val updated =
            if (stream) s.copy(streams = s.streams - 1, lastActive = now)
            else s.copy(pending = s.pending - 1, lastActive = now)
          (updated.closing && updated.isIdle, updated)
        }
      }
      .flatMap(ZIO.when(_)(entity.drained.succeed(())))
      .unit

  /**
    * Stop an entity without waiting, if `condition` holds.
    * The entity accepts no new messages once this returns, and is given up on if it doesn't terminate in time.
    */
  private def stopInBackground(
    entity: RunningEntity[Msg],
    condition: RunningEntity.State => Boolean = _ => true
  ): UIO[Unit] =
    entity.state
      .modify { s =>
        if (s.closing || !condition(s)) (ZIO.unit, s)
        else
          (
            entity.closing.succeed(()) *> ZIO.when(s.isIdle)(entity.drained.succeed(())) *> finishInBackground(entity),
            s.copy(closing = true)
          )
      }
      .flatten
      .uninterruptible

  /**
    * In the background, tell the entity to finish once its messages are completed, or after `entityTerminationTimeout`.
    * Then give up on it if it hasn't terminated after twice `entityTerminationTimeout`,
    * since the interceptor, a handler or a finalizer may never return.
    */
  private def finishInBackground(entity: RunningEntity[Msg]): UIO[Unit] =
    withRegistration(
      // interruptible so that the timeouts work
      entity.drained.await.interruptible
        .timeout(config.entityTerminationTimeout)
        .timed
        .flatMap { case (elapsed, drained) =>
          entity.mailbox.offer(Event.Finish(drained.isDefined)) *>
            entity.terminated.await.interruptible.timeout((config.entityTerminationTimeout * 2).minus(elapsed))
        }
        .someOrElseZIO(
          ZIO.logWarning(
            s"${recipientType.name} ${entity.entityId} did not terminate within twice ${config.entityTerminationTimeout.render}, leaving it in the background"
          ) *> finishMailboxes(entity, interrupted) *>
            // `finishMailboxes` also took the `Finish` sent above, so send it again in case the loop is still running
            entity.mailbox.offer(Event.Finish(drained = false)) *>
            // before removing it, so that an interceptor running the entity late can't start it next to a new instance
            entity.terminated.succeed(()) *> remove(entity)
        )
    ).forkDaemon.unit

  /**
    * Complete the messages still in the entity's mailbox with the given result.
    * Call it once the entity is closing and nothing takes messages from its mailbox anymore.
    */
  private def finishQueued(entity: RunningEntity[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    // messages accepted before closing may not be in the mailbox yet, hence the retry
    finishMailboxes(entity, exit) *>
      entity.state.get.flatMap(s => if (s.pending == 0) ZIO.unit else ZIO.yieldNow *> finishQueued(entity, exit))

  private def finishMailboxes(entity: RunningEntity[Msg], exit: Exit[Any, Any]): UIO[Unit] =
    entity.mailbox.takeAll.flatMap(events => ZIO.foreachDiscard(events.collect { case envelope: Envelope[Msg] => envelope })(finish(entity, _, exit)))

  def terminateEntity(entityId: String): UIO[Unit] =
    // don't make the caller wait, since this can be called from the entity itself
    entities.get.flatMap(_.running.get(entityId) match {
      case Some(entity) => stopInBackground(entity)
      case None         => ZIO.unit
    })

  // call it after the change that prevents the entities being stopped from starting again (e.g. unassigning their shards)
  private val collectEntitiesToStop: UIO[Map[String, RunningEntity[Msg]]] =
    entities.modify(current => (current.running, current.copy(collections = current.collections + 1)))

  /**
    * Get the IDs of the entities running on this pod, from their start until they terminate.
    */
  def entityIds: UIO[Set[String]] =
    entities.get.map(_.running.keySet)

  def terminateEntitiesOnShards(shards: Set[ShardId]): UIO[Unit] =
    recipientType match {
      case _: EntityType[?] =>
        collectEntitiesToStop.flatMap { map =>
          stopAll(map.collect {
            case (entityId, entity) if shards.contains(sharding.getShardId(recipientType, entityId)) => entity
          })
        }
      // each pod runs its own instance of a topic, whichever pod holds the shard of its name
      case _: TopicType[?]  => ZIO.unit
    }

  def terminateAllEntities: UIO[Unit] =
    collectEntitiesToStop.flatMap(map => stopAll(map.values))

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
