package shard

import shard.Sharding.ShardingRegistrationEvent
import shard.Messenger.MessengerTimeout
import shard.errors._
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.interfaces.{ Pods, Storage }
import shard.internal.{ EntityManager, Replies }
import shard.internal.EntityManager.{ Envelope, StreamReply }
import zio.{ Config => _, _ }
import zio.stream.{ ZSink, ZStream }

import java.time.OffsetDateTime

/**
 * A component that takes care of communicating with sharded entities.
 * See the companion object for layer creation and public methods.
 */
class Sharding private (
  address: PodAddress,
  config: Config,
  shardAssignments: Ref[Map[ShardId, PodAddress]],
  entityStates: Ref.Synchronized[Map[String, EntityManager[?]]],
  singletons: Ref.Synchronized[List[(String, UIO[Nothing], Option[Fiber[Nothing, Nothing]])]],
  lastUnhealthyNodeReported: Ref[OffsetDateTime],
  isShuttingDownRef: Ref[Boolean],
  shardManager: ShardManagerClient,
  pods: Pods,
  storage: Storage,
  eventsHub: Hub[ShardingRegistrationEvent]
) { self =>
  private[shard] def getShardId(recipientType: RecipientType[?], entityId: String): ShardId =
    recipientType.getShardId(entityId, config.numberOfShards)

  val register: Task[Unit] =
    ZIO.logDebug(s"Registering pod $address to Shard Manager") *>
      isShuttingDownRef.set(false) *>
      shardManager.register(address, config.role)

  val unregister: UIO[Unit] =
    (
      // ping the shard manager first to stop if it's not available
      shardManager.getAssignments(config.role) *>
        ZIO.logDebug(s"Stopping local entities") *>
        isShuttingDownRef.set(true) *>
        entityStates.get.flatMap(
          ZIO.foreachParDiscard(_) { case (name, entity) =>
            entity.terminateAllEntities.interruptible.forkDaemon // run in a daemon fiber to make sure it doesn't get interrupted
              .flatMap(_.join)
              .catchAllCause(ZIO.logErrorCause(s"Error during stop of entity $name", _))
          }
        ) *>
        ZIO.logDebug(s"Unregistering pod $address to Shard Manager") *>
        shardManager.unregister(address)
    ).retry(config.unregisterRetrySchedule)
      .catchAllCause(ZIO.logWarningCause("Error while unregistering from Shard Manager", _))

  val isSingletonNode: UIO[Boolean] =
    // Start singletons on the pod hosting shard 1.
    shardAssignments.get.map(_.get(1).contains(address))

  private def startSingletonsIfNeeded: UIO[Unit] =
    ZIO
      .whenZIODiscard(isSingletonNode) {
        singletons.updateZIO { singletons =>
          ZIO.foreach(singletons) {
            case (name, run, None) =>
              ZIO.logDebug(s"Starting singleton $name") *>
                Metrics.singletons.tagged("role", config.role.name).tagged("singleton_name", name).increment *>
                run.forkDaemon.map(fiber => (name, run, Some(fiber)))
            case other             => ZIO.succeed(other)
          }
        }
      }

  private def stopSingletonsIfNeeded: UIO[Unit] =
    ZIO
      .unlessZIODiscard(isSingletonNode) {
        singletons.updateZIO { singletons =>
          ZIO.foreach(singletons) {
            case (name, run, Some(fiber)) =>
              ZIO.logDebug(s"Stopping singleton $name") *>
                Metrics.singletons.tagged("role", config.role.name).tagged("singleton_name", name).decrement *>
                fiber.interrupt.as((name, run, None))
            case other                    => ZIO.succeed(other)
          }
        }
      }

  def registerSingleton[R](name: String, run: URIO[R, Nothing]): URIO[R, Unit] =
    ZIO.environment[R].flatMap(env => singletons.update(list => (name, run.provideEnvironment(env), None) :: list)) <*
      startSingletonsIfNeeded *>
      eventsHub.publish(ShardingRegistrationEvent.SingletonRegistered(name)).unit

  private[shard] def assign(shards: Set[ShardId]): UIO[Unit] =
    ZIO
      .unlessZIO(isShuttingDown) {
        shardAssignments.update(shards.foldLeft(_) { case (map, shard) => map.updated(shard, address) }) *>
          Metrics.shards.tagged("role", config.role.name).incrementBy(shards.size) *>
          startSingletonsIfNeeded *>
          ZIO.logDebug(s"Assigned shards: ${renderShardIds(shards)}")
      }
      .unit

  private[shard] def unassign(shards: Set[ShardId]): UIO[Unit] =
    shardAssignments.modify { map =>
      val removed = shards.filter(s => map.get(s).contains(address))
      (removed.size, map -- removed)
    }.flatMap(removedCount =>
      ZIO.logDebug(s"Unassigning shards: ${renderShardIds(shards)}") *>
        entityStates.get.flatMap(state =>
          ZIO.foreachDiscard(state.values)(
            _.terminateEntitiesOnShards(shards) // this will return once all shards are terminated
          )
        ) *>
        Metrics.shards.tagged("role", config.role.name).decrementBy(removedCount) *>
        stopSingletonsIfNeeded *>
        ZIO.logDebug(s"Unassigned shards: ${renderShardIds(shards)}")
    )

  def getPodAddress(recipientType: RecipientType[?], entityId: String): UIO[Option[PodAddress]] =
    for {
      shards <- shardAssignments.get
      shardId = getShardId(recipientType, entityId)
      pod     = shards.get(shardId)
    } yield pod

  def isEntityOnLocalShards(recipientType: RecipientType[?], entityId: String): UIO[Boolean] =
    getPodAddress(recipientType, entityId).map(pod => pod.contains(address))

  val getAssignments: UIO[Map[ShardId, PodAddress]] =
    shardAssignments.get

  val thisPodAssignments: UIO[Chunk[ShardId]] =
    getAssignments.map(a =>
      Chunk.fromIterable(a.view.collect { case (shardId, addr) if addr == this.address => shardId })
    )

  val getPods: UIO[Set[PodAddress]] =
    shardAssignments.get.map(_.values.toSet)

  private def updateAssignments(
    assignmentsOpt: Map[ShardId, Option[PodAddress]],
    replaceAllAssignments: Boolean
  ): UIO[Unit] = {
    val assignments = assignmentsOpt.flatMap { case (k, v) => v.map(k -> _) }
    ZIO.logDebug("Received new shard assignments") *>
      Metrics.shards
        .tagged("role", config.role.name)
        .set(assignmentsOpt.count { case (_, podOpt) => podOpt.contains(address) })
        .whenDiscard(replaceAllAssignments) *>
      (if (replaceAllAssignments) shardAssignments.set(assignments)
       else
         shardAssignments.update(map =>
           // we keep self assignments (we don't override them with the new assignments
           // because only the Shard Manager is able to change assignments of the current node, via assign/unassign
           assignments.filter { case (_, pod) => pod != address } ++
             map.filter { case (_, pod) => pod == address }
         ))
  }

  private[shard] val refreshAssignments: ZIO[Scope, Nothing, Unit] =
    for {
      latch           <- Promise.make[Nothing, Unit]
      assignmentStream = ZStream.fromZIO(
                           // first, get the assignments from the shard manager directly
                           shardManager.getAssignments(config.role).map(_ -> true)
                         ) ++
                           // then, get assignments changes from Redis
                           storage.assignmentsStream(config.role).map(_ -> false)
      _               <- assignmentStream.mapZIO { case (assignmentsOpt, replaceAllAssignments) =>
                           updateAssignments(assignmentsOpt, replaceAllAssignments) *> latch
                             .succeed(())
                             .whenDiscard(replaceAllAssignments)
                         }.runDrain
                           .retry(Schedule.fixed(config.refreshAssignmentsRetryInterval))
                           .interruptible
                           .forkDaemon
                           .withFinalizer(_.interrupt)
      _               <- latch.await
    } yield ()

  private[shard] def isShuttingDown: UIO[Boolean] =
    isShuttingDownRef.get

  private def entityManager(entityType: String): Task[EntityManager[?]] =
    entityStates.get.flatMap(_.get(entityType) match {
      case Some(state) => ZIO.succeed(state)
      case None        => ZIO.fail(new Exception(s"Entity type $entityType was not registered."))
    })

  private def entityManager[Msg[+_, +_]](recipientType: RecipientType[Msg]): Task[EntityManager[Msg]] =
    entityManager(recipientType.name).map(_.asInstanceOf[EntityManager[Msg]])

  // returns a reply instead if the message was not enqueued
  private def enqueueBinary(msg: BinaryMessage): Task[Either[BinaryReply, (Envelope[?], ReplyCodec)]] =
    entityManager(msg.entityType)
      .flatMap(_.enqueueBinary(msg))
      .map(Right(_))
      .catchSome {
        case _: MailboxFull               => ZIO.left(BinaryReply.MailboxFull)
        case _: MessageDiscarded          => ZIO.left(BinaryReply.Discarded)
        case _: EntityNotManagedByThisPod => ZIO.left(BinaryReply.EntityNotManagedByThisPod)
      }

  // an interruption while waiting for the reply cancels the message (but not one that happens while enqueuing it)
  private def receive[A](msg: BinaryMessage)(onReply: BinaryReply => A)(
    onExit: (Exit[Any, Any], ReplyCodec) => A
  ): Task[A] =
    ZIO.uninterruptibleMask(restore =>
      restore(enqueueBinary(msg)).flatMap {
        case Right((envelope, codec)) if msg.expectsReply => Sharding.await(envelope, restore).map(onExit(_, codec))
        case Right(_)                                     => ZIO.succeed(onReply(Sharding.ack))
        case Left(reply)                                  => ZIO.succeed(onReply(reply))
      }
    )

  private[shard] def sendToLocalEntity(msg: BinaryMessage): Task[BinaryReply] =
    receive(msg)(identity)(Replies.encodeExit)

  // stops at the first rejection so that messages stay in order
  private[shard] def sendStreamToLocalEntity(messages: ZStream[Any, Throwable, BinaryMessage]): Task[BinaryReply] =
    messages.runFoldWhileZIO(Sharding.ack)(_.isInstanceOf[BinaryReply.Success])((_, msg) => sendToLocalEntity(msg))

  // the first message is the request, the following ones are its inputs
  private[shard] def sendStreamToLocalEntityAndReceiveStream(
    messages: ZStream[Any, Throwable, BinaryMessage]
  ): ZStream[Any, Throwable, BinaryReply] =
    ZStream.unwrapScoped {
      messages.peel(ZSink.head[BinaryMessage]).map {
        case (None, _)              => ZStream.empty
        case (Some(request), input) =>
          def decodedInput(codec: ReplyCodec): ZStream[Any, Nothing, Any] =
            input.mapZIO(msg => ZIO.attempt(codec.streamInput.decode(msg.body))).orDie
          ZStream.unwrap(receive(request)(ZStream.succeed(_)) {
            case (Exit.Success(reply: StreamReply), codec) =>
              Replies.encodeStream(reply.run(decodedInput(codec)), codec)
            case (exit, codec)                             => ZStream.succeed(Replies.encodeExit(exit, codec))
          })
      }
    }

  private def handleError(ex: Throwable): ZIO[Any, Nothing, Unit] =
    ZIO
      .whenCaseDiscard(ex) { case PodUnavailable(pod) =>
        val notify = Clock.currentDateTime.flatMap(cdt =>
          lastUnhealthyNodeReported
            .updateAndGet(old =>
              if (old.plusNanos(config.unhealthyPodReportInterval.toNanos) isBefore cdt) cdt
              else old
            )
            .map(_ isEqual cdt)
        )
        ZIO.whenZIODiscard(notify)(shardManager.notifyUnhealthyPod(pod).forkDaemon)
      }

  private def isLocal(pod: PodAddress): Boolean =
    // if pod = self, shortcut and send directly without serialization
    pod == address && !config.simulateRemotePods

  private def fromTransport(error: Throwable): IO[Any, Nothing] =
    error match {
      case _: EntityNotManagedByThisPod | _: PodUnavailable => handleError(error) *> ZIO.fail(Sharding.Retry(error))
      case error: ClusterError                              => ZIO.fail(error)
      case other                                            => ZIO.die(other)
    }

  // waits until the shard is assigned
  private def podOf(shardId: ShardId): UIO[PodAddress] =
    shardAssignments.get.flatMap(_.get(shardId) match {
      case Some(pod) => ZIO.succeed(pod)
      case None      => Clock.sleep(100.millis) *> podOf(shardId)
    })

  private def sendToEntity[A](entityType: EntityType[?], entityId: String)(
    send: PodAddress => IO[Any, A]
  ): IO[Any, A] = {
    val shardId             = getShardId(entityType, entityId)
    def trySend: IO[Any, A] =
      podOf(shardId).flatMap(send(_).catchSome { case Sharding.Retry(_) => Clock.sleep(200.millis) *> trySend })

    if (shardId >= 1 && shardId <= config.numberOfShards) trySend
    else ZIO.fail(InvalidShardId(entityId, shardId))
  }

  // once the first reply was received, the stream fails with StreamCancelled instead of retrying
  private def streamFromEntity(entityType: EntityType[?], entityId: String)(
    send: PodAddress => ZStream[Any, Any, Any]
  ): ZStream[Any, Any, Any] = {
    val shardId                         = getShardId(entityType, entityId)
    def trySend: ZStream[Any, Any, Any] =
      ZStream.unwrap((Ref.make(false) <*> podOf(shardId)).map { case (started, pod) =>
        send(pod).tap(_ => started.set(true)).catchSome { case Sharding.Retry(_) =>
          ZStream.unwrap(started.get.map {
            case false => ZStream.execute(Clock.sleep(200.millis)) ++ trySend
            case true  => ZStream.fail(StreamCancelled)
          })
        }
      })

    if (shardId >= 1 && shardId <= config.numberOfShards) trySend
    else ZStream.fail(InvalidShardId(entityId, shardId))
  }

  // an interruption while waiting for the reply cancels the message (but not one that happens while enqueuing it)
  private def enqueueAndAwait[Msg[+_, +_]](
    recipientType: RecipientType[Msg],
    entityId: String,
    message: Msg[Any, Any]
  ): IO[Any, Exit[Any, Any]] =
    ZIO.uninterruptibleMask(restore =>
      restore(entityManager(recipientType).flatMap(_.enqueue(entityId, message)))
        .catchAll(fromTransport)
        .flatMap(Sharding.await(_, restore))
    )

  private def deliver[Msg[+_, +_]](
    recipientType: RecipientType[Msg],
    entityId: String,
    pod: PodAddress,
    message: Msg[Any, Any],
    expectsReply: Boolean
  ): IO[Any, Any] =
    if (isLocal(pod))
      if (expectsReply)
        enqueueAndAwait(recipientType, entityId, message)
          .flatMap(Replies.toResult(_, EntityInterrupted(recipientType.name, entityId)))
      else entityManager(recipientType).flatMap(_.enqueue(entityId, message)).catchAll(fromTransport).unit
    else
      ZIO.attempt(recipientType.codec.encodeMessage(message)).orDie.flatMap { body =>
        pods
          .sendMessage(pod, BinaryMessage(entityId, recipientType.name, body, expectsReply))
          .catchAll(fromTransport)
          .flatMap {
            case BinaryReply.Success(_) if !expectsReply => ZIO.unit
            case reply                                   =>
              Replies.decodeReply(reply, recipientType.codec.replyCodec(message), recipientType.name, entityId)
          }
      }

  private def deliverStream[Msg[+_, +_]](
    entityType: EntityType[Msg],
    entityId: String,
    pod: PodAddress,
    message: Msg[Any, Any],
    input: Option[ZStream[Any, Nothing, Any]]
  ): ZStream[Any, Any, Any] =
    if (isLocal(pod))
      ZStream.unwrap(enqueueAndAwait(entityType, entityId, message).map {
        case Exit.Success(reply: StreamReply) => Sharding.localStream(reply.run(input.getOrElse(ZStream.empty)))
        case Exit.Success(other)              =>
          ZStream.dieMessage(s"Unexpected reply $other to a stream message to ${entityType.name} $entityId")
        case exit                             =>
          ZStream.fromZIO(Replies.toResult(exit, StreamCancelled))
      })
    else
      ZStream.unwrap(ZIO.attempt(entityType.codec.encodeMessage(message)).orDie.map { body =>
        val request  = BinaryMessage(entityId, entityType.name, body, expectsReply = true)
        val codec    = entityType.codec.replyCodec(message)
        val messages = input match {
          case None        => ZStream.succeed(request)
          case Some(input) =>
            ZStream.succeed(request) ++ input.mapZIO(value =>
              ZIO
                .attempt(BinaryMessage(entityId, entityType.name, codec.streamInput.encode(value), expectsReply = true))
                .orDie
            )
        }
        Replies.decodeStream(
          pods
            .sendStreamAndReceiveStream(pod, messages)
            .catchAll(error => ZStream.fromZIO(fromTransport(error))),
          codec,
          entityType.name,
          entityId
        )
      })

  private def deliverMessages[Msg[+_, +_]](
    entityType: EntityType[Msg],
    entityId: String,
    pod: PodAddress,
    messages: ZStream[Any, Any, Msg[Any, Any]]
  ): IO[Any, Unit] =
    if (isLocal(pod))
      entityManager(entityType).catchAll(fromTransport).flatMap { manager =>
        messages.mapZIO(manager.enqueue(entityId, _).catchAll(fromTransport)).runDrain
      }
    else
      Promise.make[Any, Nothing].flatMap { inputFailure =>
        val failIfInputFailed = ZIO.whenZIODiscard(inputFailure.isDone)(inputFailure.await)
        val requests          = messages
          .catchAllCause(cause => ZStream.execute(inputFailure.failCause(cause)))
          .mapZIO(msg =>
            ZIO
              .attempt(
                BinaryMessage(entityId, entityType.name, entityType.codec.encodeMessage(msg), expectsReply = false)
              )
              // a failure (rather than a defect) cancels the call, then the sender dies with it
              .tapError(inputFailure.die(_))
          )
        pods
          .sendStream(pod, requests)
          .catchAll(error => failIfInputFailed *> fromTransport(error))
          .flatMap(Replies.decodeReply(_, Sharding.unitReplyCodec, entityType.name, entityId)) *>
          failIfInputFailed
      }

  def messenger[Msg[+_, +_]](
    entityType: EntityType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): Messenger[Msg] =
    new Messenger[Msg] {
      private val timeout = getTimeout(sendTimeout)

      def send[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[E | ClusterError, A] =
        withTimeout(timeout, entityType, entityId)(
          sendToEntity(entityType, entityId)(deliver(entityType, entityId, _, message, expectsReply = true))
        )
          .asInstanceOf[IO[E | ClusterError, A]]

      def sendDiscard[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[ClusterError, Unit] =
        withTimeout(timeout, entityType, entityId)(
          sendToEntity(entityType, entityId)(deliver(entityType, entityId, _, message, expectsReply = false))
        ).catchSome { case _: MessageDiscarded => ZIO.unit } // the entity is not running: nothing to do
          .unit
          .asInstanceOf[IO[ClusterError, Unit]]

      def stream[E, E2, A](entityId: String)(
        message: Msg[E, Streamed[E2, A]]
      ): ZStream[Any, E | E2 | ClusterError, A] =
        streamFromEntity(entityType, entityId)(deliverStream(entityType, entityId, _, message, None))
          .asInstanceOf[ZStream[Any, E | E2 | ClusterError, A]]

      def duplex[E, In, E1, E2, A](entityId: String)(
        message: Msg[E, Duplex[In, E2, A]],
        input: ZStream[Any, E1, In]
      ): ZStream[Any, E | E1 | E2 | ClusterError, A] =
        ZStream
          .unwrap(Promise.make[Any, Nothing].map { inputFailure =>
            val safeInput = input.catchAll(error => ZStream.execute(inputFailure.fail(error)))
            streamFromEntity(entityType, entityId)(deliverStream(entityType, entityId, _, message, Some(safeInput)))
              .interruptWhen(inputFailure) ++
              // the replies may end because the inputs failed
              ZStream.execute(ZIO.whenZIODiscard(inputFailure.isDone)(inputFailure.await))
          })
          .asInstanceOf[ZStream[Any, E | E1 | E2 | ClusterError, A]]

      def sendStream[E1](entityId: String)(
        messages: ZStream[Any, E1, Msg[Nothing, Unit]]
      ): IO[E1 | ClusterError, Unit] =
        sendToEntity(entityType, entityId)(deliverMessages(entityType, entityId, _, messages))
          .asInstanceOf[IO[E1 | ClusterError, Unit]]
    }

  def broadcaster[Msg[+_, +_]](
    topicType: TopicType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): Broadcaster[Msg] =
    new Broadcaster[Msg] {
      private val timeout = getTimeout(sendTimeout)

      def broadcastDiscard[E, A](topic: String)(message: Msg[E, A])(using Unary[A]): UIO[Unit] =
        sendToAllPods(topic, message, expectsReply = false).unit

      def broadcast[E, A](topic: String)(message: Msg[E, A])(using
        Unary[A]
      ): UIO[Map[PodAddress, Exit[E | ClusterError, A]]] =
        sendToAllPods(topic, message, expectsReply = true)
          .asInstanceOf[UIO[Map[PodAddress, Exit[E | ClusterError, A]]]]

      private def sendToAllPods(
        topic: String,
        message: Msg[Any, Any],
        expectsReply: Boolean
      ): UIO[Map[PodAddress, Exit[Any, Any]]] =
        getPods.flatMap(pods =>
          ZIO
            .foreachPar(pods.toList) { pod =>
              def trySend: IO[Any, Any] =
                deliver(topicType, topic, pod, message, expectsReply).catchSome {
                  case Sharding.Retry(error: PodUnavailable)        =>
                    // retry as long as the pod is registered
                    Clock.sleep(200.millis) *> getPods
                      .flatMap(pods => if (pods.contains(pod)) trySend else ZIO.fail(error))
                  case Sharding.Retry(_: EntityNotManagedByThisPod) =>
                    // the pod is shutting down
                    ZIO.fail(PodUnavailable(pod))
                }
              withTimeout(timeout, topicType, topic)(trySend).exit.map(pod -> _)
            }
            .map(_.toMap)
        )
    }

  private def withTimeout[A](timeout: Option[Duration], recipientType: RecipientType[?], entityId: String)(
    effect: IO[Any, A]
  ): IO[Any, A] =
    timeout.fold(effect)(t => effect.timeoutFail(SendTimeout(recipientType.name, entityId))(t).interruptible)

  private def getTimeout(sendTimeout: MessengerTimeout): Option[Duration] =
    sendTimeout match {
      case MessengerTimeout.NoTimeout            => None
      case MessengerTimeout.InheritConfigTimeout => Some(config.sendTimeout)
      case MessengerTimeout.Timeout(duration)    => Some(duration)
    }

  def registerEntity[R, Msg[+_, +_]](entityType: EntityType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] =
    registerRecipient(entityType, options, behavior)

  def registerTopic[R, Msg[+_, +_]](topicType: TopicType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] =
    registerRecipient(topicType, options, behavior)

  def getShardingRegistrationEvents: ZStream[Any, Nothing, ShardingRegistrationEvent] =
    ZStream.fromHub(eventsHub)

  private def registerRecipient[R, Msg[+_, +_]](
    recipientType: RecipientType[Msg],
    options: EntityOptions[Msg],
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] =
    for {
      env        <- ZIO.environment[R]
      makeHandler = (entityId: String) => behavior(entityId).provideSomeEnvironment[Scope](env ++ _).orDie
      _          <- ZIO.acquireRelease(
                      entityStates.modifyZIO(map =>
                        if (map.contains(recipientType.name))
                          ZIO.dieMessage(
                            s"An entity or topic type named ${recipientType.name} is already registered, names must be unique"
                          )
                        else
                          EntityManager
                            .make(recipientType, makeHandler, self, config, options)
                            .map(manager => (manager, map.updated(recipientType.name, manager)))
                      )
                    )(manager => manager.close *> entityStates.update(_ - recipientType.name))
      _          <- eventsHub.publish(recipientType match {
                      case entityType: EntityType[?] => ShardingRegistrationEvent.EntityRegistered(entityType)
                      case topicType: TopicType[?]   => ShardingRegistrationEvent.TopicRegistered(topicType)
                    })
    } yield ()

  def terminateLocalEntity(entityType: EntityType[?], entityId: String): UIO[Unit] =
    entityManager(entityType.name).flatMap(_.terminateEntity(entityId)).ignore
}

object Sharding {

  sealed trait ShardingRegistrationEvent

  object ShardingRegistrationEvent {
    case class EntityRegistered(entityType: EntityType[?]) extends ShardingRegistrationEvent {
      override def toString: String = s"Registered entity ${entityType.name}"
    }
    case class SingletonRegistered(name: String)           extends ShardingRegistrationEvent {
      override def toString: String = s"Registered singleton $name"
    }
    case class TopicRegistered(topicType: TopicType[?])    extends ShardingRegistrationEvent {
      override def toString: String = s"Registered topic ${topicType.name}"
    }
  }

  /**
   * A transport failure meaning the message can be sent again.
   * Not public so that a message's typed error is never retried.
   */
  private[shard] final case class Retry(error: Throwable)

  // acknowledges a message that doesn't expect a reply
  private val ack: BinaryReply = BinaryReply.Success(Array.emptyByteArray)

  private val unitReplyCodec: ReplyCodec = ReplyCodec(Codec.nothing, Codec.unit, Codec.nothing, Codec.nothing)

  // must be called within uninterruptibleMask, right after enqueuing the message
  private def await(envelope: Envelope[?], restore: ZIO.InterruptibilityRestorer): UIO[Exit[Any, Any]] =
    restore(envelope.await).onInterrupt(envelope.cancel)

  // interruptions end the stream with StreamCancelled, like for a remote entity
  private def localStream(stream: ZStream[Any, Any, Any]): ZStream[Any, Any, Any] =
    stream.catchAllCause(cause =>
      if (cause.isInterruptedOnly) ZStream.fail(StreamCancelled) else ZStream.failCause(cause)
    )

  /**
   * A layer that sets up sharding communication between pods.
   */
  val live: ZLayer[Pods with ShardManagerClient with Storage with Config, Throwable, Sharding] =
    ZLayer.scoped {
      for {
        config                    <- ZIO.service[Config]
        pods                      <- ZIO.service[Pods]
        shardManager              <- ZIO.service[ShardManagerClient]
        storage                   <- ZIO.service[Storage]
        shardsCache               <- Ref.make(Map.empty[ShardId, PodAddress])
        entityStates              <- Ref.Synchronized.make[Map[String, EntityManager[?]]](Map())
        singletons                <- Ref.Synchronized
                                       .make[List[(String, UIO[Nothing], Option[Fiber[Nothing, Nothing]])]](Nil)
                                       .withFinalizer(
                                         _.get.flatMap(singletons =>
                                           ZIO.foreach(singletons) {
                                             case (_, _, Some(fiber)) => fiber.interrupt
                                             case _                   => ZIO.unit
                                           }
                                         )
                                       )
        cdt                       <- Clock.currentDateTime
        lastUnhealthyNodeReported <- Ref.make(cdt)
        shuttingDown              <- Ref.make(false)
        eventsHub                 <- Hub.unbounded[ShardingRegistrationEvent]
        sharding                   = new Sharding(
                                       PodAddress(config.selfHost, config.shardingPort),
                                       config,
                                       shardsCache,
                                       entityStates,
                                       singletons,
                                       lastUnhealthyNodeReported,
                                       shuttingDown,
                                       shardManager,
                                       pods,
                                       storage,
                                       eventsHub
                                     )
        _                         <- sharding.getShardingRegistrationEvents.mapZIO(event => ZIO.logInfo(event.toString)).runDrain.forkDaemon
        _                         <- sharding.refreshAssignments
      } yield sharding
    }

  /**
   * Notify the shard manager that shards can now be assigned to this pod.
   */
  def register: RIO[Sharding, Unit] =
    ZIO.serviceWithZIO[Sharding](_.register)

  /**
   * Notify the shard manager that shards must be unassigned from this pod.
   */
  def unregister: URIO[Sharding, Unit] =
    ZIO.serviceWithZIO[Sharding](_.unregister)

  /**
   * Same as `register`, but will automatically call `unregister` when the `Scope` is terminated.
   */
  def registerScoped: RIO[Sharding with Scope, Unit] =
    Sharding.register.withFinalizer(_ => Sharding.unregister)

  /**
   * Returns true if current node contains the singletons
   */
  def isSingletonNode: RIO[Sharding, Boolean] =
    ZIO.serviceWithZIO[Sharding](_.isSingletonNode)

  /**
   * Start a computation that is guaranteed to run only on a single pod.
   * Each pod should call `registerSingleton` but only a single pod will actually run it at any given time.
   */
  def registerSingleton[R](name: String, run: URIO[R, Nothing]): URIO[Sharding with R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerSingleton[R](name, run))

  /**
   * Register a new entity type, allowing pods to send messages to entities of this type.
   * The `behavior` is called with the entity ID when an entity starts and returns its handler. Its `Scope` is closed
   * when the entity stops, after its last message was processed.
   * Names of entity and topic types must be unique. Closing the given `Scope` unregisters the entity type.
   */
  def registerEntity[R, Msg[+_, +_]](entityType: EntityType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Sharding & Scope & R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerEntity[R, Msg](entityType, options)(behavior))

  /**
   * Register a new topic type, allowing pods to broadcast messages to subscribers.
   * It works like `registerEntity`, with one instance per topic name on each pod.
   */
  def registerTopic[R, Msg[+_, +_]](topicType: TopicType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Sharding & Scope & R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerTopic[R, Msg](topicType, options)(behavior))

  /**
   * Get an object that allows sending messages to a given entity type.
   * You can provide a custom send timeout to override the one globally defined.
   */
  def messenger[Msg[+_, +_]](
    entityType: EntityType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): URIO[Sharding, Messenger[Msg]] =
    ZIO.serviceWith[Sharding](_.messenger(entityType, sendTimeout))

  /**
   * Get an object that allows broadcasting messages to a given topic type.
   * You can provide a custom send timeout to override the one globally defined.
   */
  def broadcaster[Msg[+_, +_]](
    topicType: TopicType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): URIO[Sharding, Broadcaster[Msg]] =
    ZIO.serviceWith[Sharding](_.broadcaster(topicType, sendTimeout))

  /**
   * Get the list of shards and the pod that holds them.
   *
   * Note: ShardId may not show up if the shard is not assigned to any pod.
   */
  def getAssignments: RIO[Sharding, Map[ShardId, PodAddress]] =
    ZIO.serviceWithZIO[Sharding](_.getAssignments)

  /**
   * Get the list of shards currently assigned to the current pod
   */
  def thisPodAssignments: RIO[Sharding, Chunk[ShardId]] =
    ZIO.serviceWithZIO[Sharding](_.thisPodAssignments)

  /**
   * Get the list of pods currently registered to the Shard Manager
   */
  def getPods: RIO[Sharding, Set[PodAddress]] =
    ZIO.serviceWithZIO[Sharding](_.getPods)

  /**
   * Terminate a given entity, without waiting: it completes the messages already received and closes its scope.
   * The next message sent to the entity starts it again.
   * This method can only be used if the entity is hosted on the current pod (otherwise it will do nothing).
   * Typically, you would use this method from inside the entity behavior to stop itself.
   */
  def terminateLocalEntity(entityType: EntityType[?], entityId: String): URIO[Sharding, Unit] =
    ZIO.serviceWithZIO[Sharding](_.terminateLocalEntity(entityType, entityId))
}
