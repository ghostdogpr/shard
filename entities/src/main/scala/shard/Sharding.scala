package shard

import shard.Messenger.MessengerTimeout
import shard.Sharding.ShardingRegistrationEvent
import shard.errors._
import shard.interfaces.MessageCodec.{ Codec, ReplyCodec }
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }
import shard.interfaces.{ Pods, ShardManagerClient, Storage }
import shard.internal.{ EntityManager, Replies, Streams }
import shard.internal.EntityManager.StreamReply
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
  entityStates: Ref.Synchronized[Map[String, Option[EntityManager[?]]]], // None once its registration has closed
  singletons: Ref.Synchronized[List[(String, UIO[Nothing], Option[Fiber[Nothing, Nothing]])]],
  lastUnhealthyNodeReported: Ref[OffsetDateTime],
  lastStaleRefresh: Ref[Long],
  isShuttingDownRef: Ref[Boolean],
  registrationRef: Ref.Synchronized[Option[String]],
  shardManager: ShardManagerClient,
  pods: Pods,
  storage: Storage,
  eventsHub: Hub[ShardingRegistrationEvent]
) { self =>
  def getShardId(recipientType: RecipientType[?], entityId: String): ShardId =
    recipientType.getShardId(entityId, config.numberOfShards)

  val register: Task[Unit] =
    for {
      _                       <- ZIO.logDebug(s"Registering pod $address to Shard Manager")
      fresh                   <- Random.nextUUID.map(_.toString)
      (registration, dropped) <-
        registrationRef.modifyZIO(current =>
          isShuttingDownRef.get.flatMap {
            case false if current.isDefined => ZIO.succeed((current.get, Set.empty[ShardId]) -> current)
            case _                          =>
              // entities start without the lock: the old shards are dropped before hosting resumes
              shardAssignments
                .modify(map => (ownAssignments(map).keySet, map -- ownAssignments(map).keySet))
                .zipLeft(isShuttingDownRef.set(false))
                .map(dropped => (fresh, dropped) -> Some(fresh))
          }
        )
      _                       <- release(dropped, dropped.size).when(dropped.nonEmpty)
      shards                  <- shardManager.register(address, config.role, registration)
      _                       <- assign(registration, shards).when(shards.nonEmpty)
    } yield ()

  val unregister: UIO[Unit] =
    registrationRef.get.flatMap(ZIO.foreachDiscard(_) { registration =>
      (
        // ping the shard manager first to stop if it's not available
        shardManager.getAssignments(config.role) *>
          ifRegistration(registration)(isShuttingDownRef.set(true))
      ).retry(config.unregisterRetrySchedule)
        .flatMap {
          case Some(_) =>
            ZIO.logDebug(s"Stopping local entities") *>
              entityManagers.flatMap(
                ZIO.foreachParDiscard(_) { case (name, entity) =>
                  entity.terminateAllEntities.interruptible.forkDaemon // run in a daemon fiber to make sure it doesn't get interrupted
                    .flatMap(_.join)
                    .catchAllCause(ZIO.logErrorCause(s"Error during stop of entity $name", _))
                }
              ) *>
              registrationRef.update(_.filterNot(_ == registration)) *>
              ZIO.logDebug(s"Unregistering pod $address to Shard Manager") *>
              shardManager.unregister(address, registration).retry(config.unregisterRetrySchedule)
          case None    => ZIO.unit
        }
        .catchAllCause(ZIO.logWarningCause("Error while unregistering from Shard Manager", _))
    })

  val isSingletonNode: UIO[Boolean] =
    // Start singletons on the pod hosting shard 1.
    shardAssignments.get.map(_.get(1).contains(address))

  // `isSingletonNode` is checked before the lock, which is held while a singleton stops,
  // and again under it, to act on the latest assignments
  private def startSingletonsIfNeeded: UIO[Unit] =
    ZIO.whenZIODiscard(isSingletonNode) {
      singletons.updateZIO { singletons =>
        ZIO.ifZIO(isSingletonNode)(
          onTrue = ZIO.foreach(singletons) {
            case (name, run, None) =>
              ZIO.logDebug(s"Starting singleton $name") *>
                Metrics.singletons.tagged("role", config.role.name).tagged("singleton_name", name).increment *>
                run.interruptible.forkDaemon.map(fiber => (name, run, Some(fiber)))
            case other             => ZIO.succeed(other)
          },
          onFalse = ZIO.succeed(singletons)
        )
      }
    }

  private def stopSingletonsIfNeeded: UIO[Unit] =
    ZIO.unlessZIODiscard(isSingletonNode) {
      singletons.updateZIO { singletons =>
        ZIO.ifZIO(isSingletonNode)(
          onTrue = ZIO.succeed(singletons),
          onFalse = ZIO.foreach(singletons) {
            case (name, run, Some(fiber)) =>
              ZIO.logDebug(s"Stopping singleton $name") *>
                Metrics.singletons.tagged("role", config.role.name).tagged("singleton_name", name).decrement *>
                fiber.interrupt.as((name, run, None))
            case other                    => ZIO.succeed(other)
          }
        )
      }
    }

  // a singleton slow to stop must not delay the processing of the assignments
  private def stopSingletonsInBackground: UIO[Unit] =
    stopSingletonsIfNeeded.forkDaemon.unit

  def registerSingleton[R](name: String, run: URIO[R, Nothing]): URIO[R, Unit] =
    ZIO.environment[R].flatMap(env => singletons.update(list => (name, run.provideEnvironment(env), None) :: list)) <*
      startSingletonsIfNeeded *>
      eventsHub.publish(ShardingRegistrationEvent.SingletonRegistered(name)).unit

  // one step, so that a new registration doesn't get shards sent to the previous one
  private def ifRegistration[A](registration: String)(f: UIO[A]): UIO[Option[A]] =
    registrationRef.modifyZIO(current => ZIO.when(current.contains(registration))(f).map(_ -> current))

  private[shard] def assign(registration: String, shards: Set[ShardId]): UIO[Boolean] =
    ifRegistration(registration)(
      ZIO.unlessZIO(isShuttingDown)(
        shardAssignments.modify { map =>
          val added = shards.count(shard => !map.get(shard).contains(address))
          (added, shards.foldLeft(map)(_.updated(_, address)))
        }
      )
    ).flatMap {
      case Some(Some(added)) =>
        Metrics.shards.tagged("role", config.role.name).incrementBy(added) *>
          startSingletonsIfNeeded *>
          ZIO.logDebug(s"Assigned shards: ${renderShardIds(shards)}").as(true)
      case Some(None)        => ZIO.succeed(true)
      case None              => ZIO.succeed(false)
    }

  private[shard] def unassign(registration: String, shards: Set[ShardId]): UIO[Boolean] =
    ifRegistration(registration)(
      shardAssignments.modify { map =>
        val removed = shards.filter(s => map.get(s).contains(address))
        (removed, map -- removed)
      }
    ).flatMap {
      case Some(removed) =>
        ZIO.logDebug(s"Unassigning shards: ${renderShardIds(shards)}") *>
          release(shards, removed.size) *>
          ZIO.logDebug(s"Unassigned shards: ${renderShardIds(shards)}").as(true)
      case None          => ZIO.succeed(false)
    }

  private def release(shards: Set[ShardId], removed: Int): UIO[Unit] =
    entityManagers.flatMap(state =>
      ZIO.foreachParDiscard(state.values)(
        _.terminateEntitiesOnShards(shards) // this will return once all shards are terminated
      )
    ) *>
      Metrics.shards.tagged("role", config.role.name).decrementBy(removed) *>
      stopSingletonsIfNeeded

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
    getAssignments.map(a => Chunk.fromIterable(ownAssignments(a).keys))

  val getPods: UIO[Set[PodAddress]] =
    shardAssignments.get.map(_.values.toSet)

  val getRegisteredPods: Task[Set[Pod]] =
    // only the storage knows the version and role of the pods, saved by the Shard Manager
    storage.getPods

  def localEntities(recipientType: RecipientType[?]): UIO[Set[String]] =
    entityStates.get.flatMap(_.get(recipientType.name).flatten.fold(ZIO.succeed(Set.empty[String]))(_.entityIds))

  private def ownAssignments(assignments: Map[ShardId, PodAddress]): Map[ShardId, PodAddress] =
    assignments.filter { case (_, pod) => pod == address }

  /**
   * Update the assignments of other pods, unless they changed since `readFrom` when it is given.
   */
  private def updateAssignments(
    assignmentsOpt: Map[ShardId, Option[PodAddress]],
    readFrom: Option[Map[ShardId, PodAddress]] = None
  ): UIO[Unit] = {
    val assignments = assignmentsOpt.flatMap { case (k, v) => v.map(k -> _) }
    ZIO.logDebug("Received new shard assignments") *>
      // under the lock, so that the flag is the one of the registration whose assignments are updated
      registrationRef
        .modifyZIO(current =>
          isShuttingDown.flatMap { shuttingDown =>
            shardAssignments.modify {
              case map if readFrom.exists(_ ne map) => (0, map)
              case map                              =>
                // we keep self assignments (we don't override them with the new assignments)
                // because only the Shard Manager is able to change assignments of the current node, via assign/unassign.
                // Once the pod unregistered, the Shard Manager takes its shards back without calling unassign,
                // so we drop the self assignments that the new assignments no longer give to this pod.
                val (self, dropped) = ownAssignments(map).partition { case (shard, _) =>
                  !shuttingDown || assignments.get(shard).contains(address)
                }
                (dropped.size, assignments.filter { case (_, pod) => pod != address } ++ self)
            }
          }.map(_ -> current)
        )
        .flatMap(removed =>
          ZIO
            .when(removed > 0)(
              Metrics.shards.tagged("role", config.role.name).decrementBy(removed) *>
                stopSingletonsInBackground *>
                ZIO.logDebug(s"Removed $removed shards taken back by the Shard Manager")
            )
            .unit
        )
  }

  // a message can't reach its entity: a notification of the storage may have been missed (e.g. while reconnecting)
  private def refreshStaleAssignments: UIO[Unit] =
    Clock.nanoTime
      .flatMap(now =>
        lastStaleRefresh.modify(last =>
          if (now - last >= config.refreshAssignmentsRetryInterval.toNanos) (true, now) else (false, last)
        )
      )
      .flatMap(
        ZIO.whenDiscard(_)(
          // a notification applied during the read may be newer
          shardAssignments.get.flatMap { before =>
            storage.getAssignments(config.role).flatMap(updateAssignments(_, readFrom = Some(before)))
          }.ignore.forkDaemon
        )
      )

  private[shard] val refreshAssignments: ZIO[Scope, Nothing, Unit] =
    for {
      latch <- Promise.make[Nothing, Unit]
      _     <- (
                 // first, get the assignments from the shard manager directly
                 // (after a reconnection, a snapshot may predate an assign/unassign it hasn't recorded yet)
                 shardManager.getAssignments(config.role).flatMap(updateAssignments(_)) *>
                   latch.succeed(()) *>
                   // then, get assignments changes from Redis
                   storage.assignmentsStream(config.role).mapZIO(updateAssignments(_)).runDrain
               )
                 .retry(Schedule.fixed(config.refreshAssignmentsRetryInterval))
                 .interruptible
                 .forkDaemon
                 .withFinalizer(_.interrupt)
      _     <- latch.await
    } yield ()

  private[shard] def isShuttingDown: UIO[Boolean] =
    isShuttingDownRef.get

  private[shard] def registration: UIO[Option[String]] =
    registrationRef.get

  private def entityManagers: UIO[Map[String, EntityManager[?]]] =
    entityStates.get.map(_.collect { case (name, Some(manager)) => name -> manager })

  private def entityManager(entityType: String, entityId: String): Task[EntityManager[?]] =
    entityStates.get.flatMap(_.get(entityType) match {
      case Some(Some(manager)) => ZIO.succeed(manager)
      // other pods may still send messages to a stopping pod until they see the new assignments
      case Some(None)          => ZIO.fail(EntityNotManagedByThisPod(entityId))
      case None                => ZIO.fail(new Exception(s"Entity type $entityType was not registered."))
    })

  private def entityManager[Msg[_, _]](recipientType: RecipientType[Msg], entityId: String): Task[EntityManager[Msg]] =
    entityManager(recipientType.name, entityId).map(_.asInstanceOf[EntityManager[Msg]])

  private def receive[A](msg: BinaryMessage)(onReply: BinaryReply => A)(
    onExit: (Exit[Any, Any], ReplyCodec) => A
  ): Task[A] =
    entityManager(msg.entityType, msg.entityId)
      .flatMap(_.receive(msg)(onReply(Sharding.ack))(onExit))
      .catchSome {
        case _: MailboxFull               => ZIO.succeed(onReply(BinaryReply.MailboxFull))
        case _: MessageDiscarded          => ZIO.succeed(onReply(BinaryReply.Discarded))
        case _: EntityNotManagedByThisPod => ZIO.succeed(onReply(BinaryReply.EntityNotManagedByThisPod))
        case error: SerializationError    => ZIO.succeed(onReply(BinaryReply.SerializationError(error.message)))
      }

  private[shard] def sendToLocalEntity(msg: BinaryMessage): Task[BinaryReply] =
    receive(msg)(identity)(Replies.encodeExit)

  /**
   * Deliver a stream of messages to a local entity, in order.
   * Stops at the first rejected message and returns its reply.
   */
  private[shard] def sendStreamToLocalEntity(messages: ZStream[Any, Throwable, BinaryMessage]): Task[BinaryReply] =
    Streams.run(
      messages,
      ZSink.foldZIO(Sharding.ack)(_.isInstanceOf[BinaryReply.Success])((_, msg) => sendToLocalEntity(msg))
    )

  /**
   * Deliver a message with its stream of inputs to a local entity and return the stream of replies.
   * The first message is the request and the next ones are its inputs.
   */
  private[shard] def sendStreamToLocalEntityAndReceiveStream(
    messages: ZStream[Any, Throwable, BinaryMessage]
  ): ZStream[Any, Throwable, BinaryReply] =
    ZStream.unwrapScoped {
      messages.peel(ZSink.head[BinaryMessage]).map {
        case (None, _)              => ZStream.empty
        case (Some(request), input) =>
          // an input that fails to decode ends the inputs, then fails the replies
          def replies(reply: StreamReply, codec: ReplyCodec): ZStream[Any, Any, Any] =
            ZStream.unwrap(Promise.make[Any, Nothing].map { inputFailure =>
              val decodedInput = Replies
                .mapElements(input.orDie, "decode a stream input")(msg => codec.streamInput.decode(msg.body))
                .catchAll(error => ZStream.execute(inputFailure.fail(error)))
              Streams.interruptWhen(reply.run(decodedInput), inputFailure.await) ++
                ZStream.execute(ZIO.whenZIODiscard(inputFailure.isDone)(inputFailure.await))
            })
          ZStream.unwrap(receive(request)(ZStream.succeed(_)) {
            case (Exit.Success(reply: StreamReply), codec) =>
              // without inputs, nothing can fail them: no need to watch for that on every element
              Replies.encodeStream(if (reply.usesInput) replies(reply, codec) else reply.run(ZStream.empty), codec)
            case (exit, codec)                             => ZStream.succeed(Replies.encodeExit(exit, codec))
          })
      }
    }

  private def reportUnhealthy(pod: PodAddress): UIO[Unit] = {
    val notify = Clock.currentDateTime.flatMap(cdt =>
      lastUnhealthyNodeReported
        .updateAndGet(old =>
          if (old.plusNanos(config.unhealthyPodReportInterval.toNanos).isBefore(cdt)) cdt
          else old
        )
        .map(_.isEqual(cdt))
    )
    ZIO.whenZIODiscard(notify)(shardManager.notifyUnhealthyPod(pod).forkDaemon)
  }

  private def isLocal(pod: PodAddress): Boolean =
    // if pod = self, shortcut and send directly without serialization
    pod == address && !config.simulateRemotePods

  private def fromTransport(error: Throwable): IO[Any, Nothing] =
    error match {
      case PodUnavailable(pod)          => reportUnhealthy(pod) *> ZIO.fail(Sharding.Retry(error))
      case _: EntityNotManagedByThisPod => ZIO.fail(Sharding.Retry(error))
      case error: ClusterError          => ZIO.fail(error)
      case other                        => ZIO.fail(TransportError(other.toString))
    }

  /**
   * Get the pod that hosts an entity, waiting until its shard is assigned.
   */
  private def podOf(entityType: EntityType[?], entityId: String): IO[InvalidShardId, PodAddress] =
    shardAssignments.get.flatMap { assignments =>
      val shardId = getShardId(entityType, entityId)
      if (shardId < 1 || shardId > config.numberOfShards) ZIO.fail(InvalidShardId(entityId, shardId))
      else
        assignments.get(shardId) match {
          case Some(pod) => ZIO.succeed(pod)
          case None      => refreshStaleAssignments *> Clock.sleep(100.millis) *> podOf(entityType, entityId)
        }
    }

  private def sendToEntity[A](entityType: EntityType[?], entityId: String)(
    send: PodAddress => IO[Any, A]
  ): IO[Any, A] =
    podOf(entityType, entityId).flatMap(pod =>
      send(pod).catchSome { case Sharding.Retry(_) =>
        refreshStaleAssignments.unless(pod == address) *> Clock.sleep(200.millis) *> sendToEntity(entityType, entityId)(
          send
        )
      }
    )

  /**
   * Receive a stream from the pod that hosts an entity, retrying if the pod can't be reached or no longer hosts it.
   * Until a reply was received or the inputs were read, it is also sent again if the entity stops.
   * After that, the stream fails with `StreamCancelled` instead of retrying.
   */
  private def streamFromEntity(
    entityType: EntityType[?],
    entityId: String,
    inputRead: UIO[Boolean] = ZIO.succeed(false)
  )(send: PodAddress => ZStream[Any, Any, Any]): ZStream[Any, Any, Any] =
    ZStream.unwrap((Ref.make(false) <*> podOf(entityType, entityId)).map { case (started, pod) =>
      def retryUnless(cancelled: UIO[Boolean]): ZStream[Any, Any, Any] =
        ZStream.unwrap(cancelled.map {
          case false =>
            ZStream.execute(Clock.sleep(200.millis)) ++ streamFromEntity(entityType, entityId, inputRead)(send)
          case true  => ZStream.fail(StreamCancelled)
        })

      send(pod).mapChunksZIO(chunk => started.set(true).when(chunk.nonEmpty).as(chunk)).catchSome {
        case Sharding.Retry(_) | StreamCancelled =>
          ZStream.execute(refreshStaleAssignments.unless(pod == address)) ++ retryUnless(
            started.get.zipWith(inputRead)(_ || _)
          )
      }
    })

  private def encode[Msg[_, _], E, A](
    recipientType: RecipientType[Msg],
    message: Msg[E, A]
  ): IO[SerializationError, (Array[Byte], ReplyCodec)] =
    ZIO
      .attempt((recipientType.codec.encodeMessage(message), recipientType.codec.replyCodec(message)))
      .mapError(Replies.serializationError("encode a message", _))

  private def deliver[Msg[_, _], E, A](
    recipientType: RecipientType[Msg],
    entityId: String,
    pod: PodAddress,
    message: Msg[E, A],
    expectsReply: Boolean
  ): IO[Any, Any] =
    if (isLocal(pod))
      if (expectsReply)
        entityManager(recipientType, entityId)
          .flatMap(_.ask(entityId, message))
          .catchAll(fromTransport)
          .flatMap(Replies.toResult(_, EntityInterrupted(recipientType.name, entityId)))
      else entityManager(recipientType, entityId).flatMap(_.enqueue(entityId, message)).catchAll(fromTransport).unit
    else
      encode(recipientType, message).flatMap { case (body, codec) =>
        pods
          .sendMessage(pod, BinaryMessage(entityId, recipientType.name, body, expectsReply))
          .catchAll(fromTransport)
          .flatMap {
            case BinaryReply.Success(_) if !expectsReply => ZIO.unit
            case reply                                   => Replies.decodeReply(reply, codec, recipientType.name, entityId)
          }
      }

  private def deliverStream[Msg[_, _], E, A](
    entityType: EntityType[Msg],
    entityId: String,
    pod: PodAddress,
    message: Msg[E, A],
    input: ZStream[Any, Nothing, Any],
    failInput: SerializationError => UIO[Any]
  ): ZStream[Any, Any, Any] =
    if (isLocal(pod))
      ZStream.unwrap(entityManager(entityType, entityId).flatMap(_.ask(entityId, message)).catchAll(fromTransport).map {
        case Exit.Success(reply: StreamReply) => Sharding.localStream(reply.run(input))
        case exit                             => ZStream.fromZIO(Replies.toResult(exit, StreamCancelled))
      })
    else
      ZStream.unwrap(encode(entityType, message).map { case (body, codec) =>
        val request  = BinaryMessage(entityId, entityType.name, body, expectsReply = true)
        // an input that fails to encode ends the inputs, like an input failure
        val messages = ZStream.succeed(request) ++ Replies
          .mapElements(input, "encode a stream input")(value =>
            BinaryMessage(entityId, entityType.name, codec.streamInput.encode(value), expectsReply = true)
          )
          .catchAll(error => ZStream.execute(failInput(error)))
        Replies.decodeStream(
          pods
            .sendStreamAndReceiveStream(pod, messages)
            .catchAll(error => ZStream.fromZIO(fromTransport(error))),
          codec,
          entityType.name,
          entityId
        )
      })

  private def deliverMessages[Msg[_, _]](
    entityType: EntityType[Msg],
    entityId: String,
    pod: PodAddress,
    messages: ZStream[Any, Any, Msg[Nothing, Unit]]
  ): IO[Any, Unit] =
    if (isLocal(pod))
      // an empty stream never looks up the entity type, like on a remote pod
      Streams.run(
        messages.mapChunksZIO(chunk =>
          ZIO
            .when(chunk.nonEmpty)(
              entityManager(entityType, entityId)
                .flatMap(manager => ZIO.foreachDiscard(chunk)(manager.enqueue(entityId, _)))
                .catchAll(fromTransport)
            )
            .as(Chunk.empty)
        ),
        ZSink.drain
      )
    else
      Promise.make[Any, Nothing].flatMap { inputFailure =>
        val failIfInputFailed = ZIO.whenZIODiscard(inputFailure.isDone)(inputFailure.await)
        // a message that fails to encode ends the messages, like a failure of the stream
        val requests          = Streams.failInto(
          Replies.mapElements(messages, "encode a message")(msg =>
            BinaryMessage(entityId, entityType.name, entityType.codec.encodeMessage(msg), expectsReply = false)
          ),
          inputFailure
        )
        pods
          .sendStream(pod, requests)
          .catchAll(error => failIfInputFailed *> fromTransport(error))
          .flatMap(Replies.decodeReply(_, Sharding.unitReplyCodec, entityType.name, entityId)) *>
          failIfInputFailed
      }

  def messenger[Msg[_, _]](
    entityType: EntityType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): Messenger[Msg] =
    new Messenger[Msg] {
      private val timeout = sendTimeout match {
        case MessengerTimeout.NoTimeout            => None
        case MessengerTimeout.InheritConfigTimeout => Some(config.sendTimeout)
        case MessengerTimeout.Timeout(duration)    => Some(duration)
      }

      def send[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[E | ClusterError, A] = {
        val sent = withTimeout(timeout, entityType, entityId)(
          sendToEntity(entityType, entityId)(deliver(entityType, entityId, _, message, expectsReply = true))
        )
        (if (timeout.isDefined) sent.interruptible else sent).asInstanceOf[IO[E | ClusterError, A]]
      }

      def sendDiscard[E, A](entityId: String)(message: Msg[E, A])(using Unary[A]): IO[ClusterError, Unit] =
        withTimeout(timeout, entityType, entityId)(
          sendToEntity(entityType, entityId)(deliver(entityType, entityId, _, message, expectsReply = false))
        ).catchSome { case _: MessageDiscarded => ZIO.unit } // the entity is not running: nothing to do
          .unit
          .asInstanceOf[IO[ClusterError, Unit]]

      def stream[E, E2, A](entityId: String)(
        message: Msg[E, Streamed[E2, A]]
      ): ZStream[Any, E | E2 | ClusterError, A] =
        streamFromEntity(entityType, entityId)(
          deliverStream(entityType, entityId, _, message, ZStream.empty, _ => ZIO.unit)
        )
          .asInstanceOf[ZStream[Any, E | E2 | ClusterError, A]]

      def duplex[E, In, E1, E2, A](entityId: String)(
        message: Msg[E, Duplex[In, E2, A]],
        input: ZStream[Any, E1, In]
      ): ZStream[Any, E | E1 | E2 | ClusterError, A] =
        ZStream
          .unwrap((Promise.make[Any, Nothing] <*> Ref.make(false)).map { case (inputFailure, inputRead) =>
            // set before the first pull, since the input may consume elements while buffering a chunk
            val safeInput = Streams.failInto(ZStream.execute(inputRead.set(true)) ++ input, inputFailure)
            Streams.interruptWhen(
              streamFromEntity(entityType, entityId, inputRead.get)(
                deliverStream(entityType, entityId, _, message, safeInput, inputFailure.fail(_))
              ),
              inputFailure.await
            ) ++
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

  def broadcaster[Msg[_, _]](
    topicType: TopicType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): Broadcaster[Msg] =
    new Broadcaster[Msg] {
      private val timeout = sendTimeout match {
        case MessengerTimeout.NoTimeout            => None
        case MessengerTimeout.InheritConfigTimeout => Some(config.sendTimeout)
        case MessengerTimeout.Timeout(duration)    => Some(duration)
      }

      def broadcastDiscard[E, A](topic: String)(message: Msg[E, A])(using Unary[A]): UIO[Unit] =
        sendToAllPods(topic, message, expectsReply = false).unit

      def broadcast[E, A](topic: String)(message: Msg[E, A])(using
        Unary[A]
      ): UIO[Map[PodAddress, Exit[E | ClusterError, A]]] =
        sendToAllPods(topic, message, expectsReply = true).interruptible
          .asInstanceOf[UIO[Map[PodAddress, Exit[E | ClusterError, A]]]]

      private def sendToAllPods[E, A](
        topic: String,
        message: Msg[E, A],
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
    // interruptible so that the timeout works in an uninterruptible region
    // exit so that a failure doesn't fail the timeout's child fiber, which ZIO would log as unhandled
    timeout.fold(effect)(t =>
      effect.exit.interruptible.timeoutFail(SendTimeout(recipientType.name, entityId))(t).flatten
    )

  def registerEntity[R, Msg[_, _]](entityType: EntityType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] =
    registerRecipient(entityType, options, behavior) *>
      eventsHub.publish(ShardingRegistrationEvent.EntityRegistered(entityType)).unit

  def registerTopic[R, Msg[_, _]](topicType: TopicType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] = registerRecipient(topicType, options, behavior) *>
    eventsHub.publish(ShardingRegistrationEvent.TopicRegistered(topicType)).unit

  def getShardingRegistrationEvents: ZStream[Any, Nothing, ShardingRegistrationEvent] =
    ZStream.fromHub(eventsHub)

  private def registerRecipient[R, Msg[_, _]](
    recipientType: RecipientType[Msg],
    options: EntityOptions[Msg],
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Scope & R, Unit] =
    for {
      env        <- ZIO.environment[R]
      makeHandler = (entityId: String) => ZIO.suspend(behavior(entityId)).provideSomeEnvironment[Scope](env ++ _).orDie
      _          <- ZIO.acquireRelease(
                      entityStates.modifyZIO(map =>
                        if (map.get(recipientType.name).flatten.nonEmpty)
                          ZIO.dieMessage(
                            s"An entity or topic type named ${recipientType.name} is already registered, names must be unique"
                          )
                        else
                          EntityManager
                            .make(recipientType, makeHandler, self, config, options)
                            .map(manager => (manager, map.updated(recipientType.name, Some(manager))))
                      )
                    )(manager => manager.close *> entityStates.update(_.updated(recipientType.name, None)))
    } yield ()

  def terminateLocalEntity(entityType: EntityType[?], entityId: String): UIO[Unit] =
    entityManager(entityType.name, entityId).flatMap(_.terminateEntity(entityId)).ignore
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

  // a transport failure after which the message can be sent again (private so that typed errors are never retried)
  private[shard] final case class Retry(error: Throwable)

  // acknowledges a message that doesn't expect a reply
  private val ack: BinaryReply = BinaryReply.Success(Array.emptyByteArray)

  private val unitReplyCodec: ReplyCodec = ReplyCodec(Codec.nothing, Codec.unit, Codec.nothing, Codec.nothing)

  /**
   * Make a stream of replies from a local entity fail like a remote one.
   * An interruption becomes `StreamCancelled` and a defect becomes `EntityDefect`.
   */
  private def localStream(stream: ZStream[Any, Any, Any]): ZStream[Any, Any, Any] =
    stream.catchAllCause(cause => ZStream.fail(Replies.toError(cause, StreamCancelled)))

  /**
   * A layer that sets up sharding communication between pods.
   */
  val live: ZLayer[Pods & ShardManagerClient & Storage & Config, Throwable, Sharding] =
    ZLayer.scoped {
      for {
        config                    <- ZIO.service[Config]
        pods                      <- ZIO.service[Pods]
        shardManager              <- ZIO.service[ShardManagerClient]
        storage                   <- ZIO.service[Storage]
        shardsCache               <- Ref.make(Map.empty[ShardId, PodAddress])
        entityStates              <- Ref.Synchronized.make[Map[String, Option[EntityManager[?]]]](Map())
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
        lastStaleRefresh          <- Clock.nanoTime.flatMap(Ref.make(_))
        shuttingDown              <- Ref.make(false)
        registration              <- Ref.Synchronized.make(Option.empty[String])
        eventsHub                 <- Hub.unbounded[ShardingRegistrationEvent]
        sharding                   = new Sharding(
                                       PodAddress(config.selfHost, config.shardingPort),
                                       config,
                                       shardsCache,
                                       entityStates,
                                       singletons,
                                       lastUnhealthyNodeReported,
                                       lastStaleRefresh,
                                       shuttingDown,
                                       registration,
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
  def registerScoped: RIO[Sharding & Scope, Unit] =
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
  def registerSingleton[R](name: String, run: URIO[R, Nothing]): URIO[Sharding & R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerSingleton[R](name, run))

  /**
   * Register a new entity type, allowing pods to send messages to entities of this type.
   * The `behavior` creates an entity's handler from its ID, and its `Scope` closes when the entity stops.
   */
  def registerEntity[R, Msg[_, _]](entityType: EntityType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Sharding & Scope & R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerEntity[R, Msg](entityType, options)(behavior))

  /**
   * Register a new topic type, allowing pods to broadcast messages to subscribers.
   * It works like `registerEntity`, with one instance per topic on each pod.
   */
  def registerTopic[R, Msg[_, _]](topicType: TopicType[Msg], options: EntityOptions[Msg] = EntityOptions[Msg]())(
    behavior: String => ZIO[R & Scope, Throwable, EntityHandler[Msg]]
  ): URIO[Sharding & Scope & R, Unit] =
    ZIO.serviceWithZIO[Sharding](_.registerTopic[R, Msg](topicType, options)(behavior))

  /**
   * Get an object that allows sending messages to a given entity type.
   * You can provide a custom send timeout to override the one globally defined.
   */
  def messenger[Msg[_, _]](
    entityType: EntityType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): URIO[Sharding, Messenger[Msg]] =
    ZIO.serviceWith[Sharding](_.messenger(entityType, sendTimeout))

  /**
   * Get an object that allows broadcasting messages to a given topic type.
   * You can provide a custom send timeout to override the one globally defined.
   */
  def broadcaster[Msg[_, _]](
    topicType: TopicType[Msg],
    sendTimeout: MessengerTimeout = MessengerTimeout.InheritConfigTimeout
  ): URIO[Sharding, Broadcaster[Msg]] =
    ZIO.serviceWith[Sharding](_.broadcaster(topicType, sendTimeout))

  /**
   * Get the shard of an entity or topic, as computed by its type's `getShardId` with the configured number of shards.
   */
  def getShardId(recipientType: RecipientType[?], entityId: String): URIO[Sharding, ShardId] =
    ZIO.serviceWith[Sharding](_.getShardId(recipientType, entityId))

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
   * Get the list of pods currently holding shards of this pod's role
   */
  def getPods: RIO[Sharding, Set[PodAddress]] =
    ZIO.serviceWithZIO[Sharding](_.getPods)

  /**
   * Get the pods registered to the Shard Manager, for every role, with their version and role.
   */
  def getRegisteredPods: RIO[Sharding, Set[Pod]] =
    ZIO.serviceWithZIO[Sharding](_.getRegisteredPods)

  /**
   * Get the IDs of the entities of a given type running on this pod, or the topics for a topic type.
   * An entity is included from its start until its `Scope` is closed.
   */
  def localEntities(recipientType: RecipientType[?]): URIO[Sharding, Set[String]] =
    ZIO.serviceWithZIO[Sharding](_.localEntities(recipientType))

  /**
   * Terminate a local entity after it processes the messages it already received, without waiting for it.
   * It does nothing if the entity is not on this pod, and the next message sent to it starts it again.
   */
  def terminateLocalEntity(entityType: EntityType[?], entityId: String): URIO[Sharding, Unit] =
    ZIO.serviceWithZIO[Sharding](_.terminateLocalEntity(entityType, entityId))
}
