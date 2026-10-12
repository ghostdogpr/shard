package shard.internal

import shard._
import shard.errors.EntityNotManagedByThisPod
import zio.{ Config => _, _ }

import java.util.concurrent.TimeUnit

private[shard] trait EntityManager[-Req] {
  def send(
    entityId: String,
    req: Req,
    replyId: Option[String],
    pendingReply: Option[PendingReply]
  ): IO[EntityNotManagedByThisPod, Unit]
  def terminateEntity(entityId: String): UIO[Unit]
  def terminateEntitiesOnShards(shards: Set[ShardId]): UIO[Unit]
  def terminateAllEntities: UIO[Unit]
}

private[shard] object EntityManager {
  private type Signal = Promise[Nothing, Unit]

  /**
   * A running entity: its queue, the fiber running its behavior and, once termination has started, the signal the
   * entity completes when it is done.
   */
  private final case class Entry[Req](
    queue: Queue[Req],
    fiber: Fiber.Runtime[Throwable, Unit],
    termination: Option[Signal]
  )

  def make[R, Req: Tag](
    recipientType: RecipientType[Req],
    behavior: (String, Queue[Req]) => RIO[R, Nothing],
    terminateMessage: Signal => Option[Req],
    sharding: Sharding,
    config: Config,
    entityMaxIdleTime: Option[Duration],
    loadEntity: Req => Boolean
  ): URIO[R, EntityManager[Req]] =
    for {
      entities               <- Ref.Synchronized.make[Map[String, Entry[Req]]](Map())
      entitiesLastReceivedAt <- Ref.make[Map[String, EpochMillis]](Map())
      env                    <- ZIO.environment[R]
    } yield new EntityManagerLive[Req](
      recipientType,
      (entityId: String, queue: Queue[Req]) => behavior(entityId, queue).provideEnvironment(env),
      terminateMessage,
      entities,
      entitiesLastReceivedAt,
      sharding,
      config,
      entityMaxIdleTime,
      loadEntity
    )

  private val currentTimeInMilliseconds: UIO[EpochMillis] =
    Clock.currentTime(TimeUnit.MILLISECONDS)

  private class EntityManagerLive[Req](
    recipientType: RecipientType[Req],
    behavior: (String, Queue[Req]) => Task[Nothing],
    terminateMessage: Signal => Option[Req],
    entities: Ref.Synchronized[Map[String, Entry[Req]]],
    entitiesLastReceivedAt: Ref[Map[String, EpochMillis]],
    sharding: Sharding,
    config: Config,
    entityMaxIdleTime: Option[Duration],
    loadEntity: Req => Boolean
  ) extends EntityManager[Req] {
    private val gauge = Metrics.entities.tagged("role", config.role.name).tagged("type", recipientType.name)

    private def startExpirationFiber(entityId: String): UIO[Fiber[Nothing, Unit]] = {
      val maxIdleTime = entityMaxIdleTime getOrElse config.entityMaxIdleTime

      def sleep(duration: Duration): UIO[Unit] =
        (Clock.sleep(duration) *> currentTimeInMilliseconds <*> entitiesLastReceivedAt.get).flatMap { case (cdt, map) =>
          val lastReceivedAt = map.getOrElse(entityId, 0L)
          val remaining      = maxIdleTime.minus(Duration.fromMillis(cdt - lastReceivedAt))
          // do not use ZIO.when to prevent zio stack memory leak
          if (remaining > Duration.Zero) sleep(remaining) else ZIO.unit
        }

      (for {
        _ <- sleep(maxIdleTime)
        _ <- terminateEntity(entityId).forkDaemon.unit // fork daemon otherwise it will interrupt itself
      } yield ()).interruptible.forkDaemon
    }

    def terminateEntity(entityId: String): UIO[Unit] =
      entities
        .modifyZIO(map =>
          map.get(entityId) match {
            case Some(entry @ Entry(queue, fiber, None)) =>
              Promise
                .make[Nothing, Unit]
                .flatMap { p =>
                  terminateMessage(p) match {
                    case Some(msg) =>
                      // if a queue is found, offer the termination message, and mark the entity as terminating so that no new message is enqueued
                      queue
                        .offer(msg)
                        .exit
                        .as((List(p -> fiber), map.updated(entityId, entry.copy(termination = Some(p)))))
                    case None      =>
                      queue.shutdown.as((Nil, map - entityId))
                  }
                }
            case _                                       =>
              // if no running entity is found, do nothing
              ZIO.succeed((Nil, map))
          }
        )
        // interrupt the entity if it doesn't terminate in time, without making the caller wait
        .flatMap(terminating => awaitTermination(terminating).forkDaemon.when(terminating.nonEmpty).unit)

    /**
     * Waits for entities to complete their termination signal, and interrupts the ones that didn't after
     * `entityTerminationTimeout`, so that an entity never keeps running once its shard has moved.
     */
    private def awaitTermination(terminating: List[(Signal, Fiber.Runtime[Throwable, Unit])]): UIO[Unit] =
      // interruptible so that the timeout works even when termination starts from an uninterruptible region (e.g. a finalizer)
      ZIO.foreachDiscard(terminating)(_._1.await).interruptible.timeout(config.entityTerminationTimeout).flatMap {
        case Some(_) => ZIO.unit
        case None    =>
          ZIO.filter(terminating) { case (p, fiber) => (p.isDone <*> fiber.poll).map(_ == (false, None)) }.flatMap {
            stuck =>
              ZIO.logWarning(
                s"${stuck.size} entities of type ${recipientType.name} did not terminate within ${config.entityTerminationTimeout.render}, interrupting them"
              ) *> Fiber.interruptAll(stuck.map(_._2))
          }
      }

    def send(
      entityId: String,
      req: Req,
      replyId: Option[String],
      pendingReply: Option[PendingReply]
    ): IO[EntityNotManagedByThisPod, Unit] =
      for {
        // first, verify that this entity should be handled by this pod
        _   <- recipientType match {
                 case _: EntityType[_] =>
                   ZIO.unlessZIO(sharding.isEntityOnLocalShards(recipientType, entityId))(
                     ZIO.fail(EntityNotManagedByThisPod(entityId))
                   )
                 case _: TopicType[_]  => ZIO.unit
               }
        // find the queue for that entity, or create it if needed
        map <- entities.get
        _   <- map.get(entityId) match {
                 case Some(Entry(queue, _, None)) =>
                   offerToQueue(entityId, queue, req, replyId, pendingReply)
                 case None if !loadEntity(req)    =>
                   pendingReply.fold[UIO[Unit]](ZIO.unit)(_.end)
                 case _                           =>
                   getOrCreateQueue(entityId).flatMap {
                     case Right(_)    =>
                       // the queue is shutting down, try again a little later
                       Clock.sleep(100 millis) *> send(entityId, req, replyId, pendingReply)
                     case Left(queue) =>
                       offerToQueue(entityId, queue, req, replyId, pendingReply)
                   }
               }
      } yield ()

    private def offerToQueue(
      entityId: String,
      queue: Queue[Req],
      req: Req,
      replyId: Option[String],
      pendingReply: Option[PendingReply]
    ): IO[EntityNotManagedByThisPod, Unit] =
      currentTimeInMilliseconds.flatMap(cdt => entitiesLastReceivedAt.update(_ + (entityId -> cdt))) *>
        // add the message to the queue and setup the reply channel if needed
        ((replyId, pendingReply) match {
          case (Some(replyId), Some(pr)) => sharding.initReply(replyId, pr) <* queue.offer(req)
          case (None, Some(pr))          => queue.offer(req) *> pr.end
          // stream continuation: reply is already set up on a prior message — just enqueue
          case (_, None)                 => queue.offer(req).unit
        }).catchAllCause(_ => Clock.sleep(100 millis) *> send(entityId, req, replyId, pendingReply))

    private def getOrCreateQueue(entityId: String): IO[EntityNotManagedByThisPod, Either[Queue[Req], Signal]] =
      entities.modifyZIO(map =>
        map.get(entityId) match {
          case Some(Entry(queue, _, None)) =>
            // the queue already exists, return it
            ZIO.succeed((Left(queue), map))
          case Some(Entry(_, _, Some(p)))  =>
            // the queue is shutting down, stash and retry
            ZIO.succeed((Right(p), map))
          case None                        =>
            sharding.isShuttingDown.flatMap {
              case true  =>
                // don't start any fiber while sharding is shutting down
                ZIO.fail(EntityNotManagedByThisPod(entityId))
              case false =>
                // queue doesn't exist, create a new one
                Queue.unbounded[Req].flatMap { queue =>
                  (for {
                    // start the expiration fiber
                    expirationFiber <- startExpirationFiber(entityId)
                    _               <- gauge.increment
                    _               <- behavior(entityId, queue)
                                         .ensuring(
                                           // shutdown the queue when the fiber ends
                                           removeEntry(entityId) *>
                                             gauge.decrement *>
                                             queue.shutdown *>
                                             expirationFiber.interrupt
                                         )
                  } yield ()).interruptible.forkDaemon // must stay interruptible to be stopped after entityTerminationTimeout
                    .map(fiber => (Left(queue), map.updated(entityId, Entry(queue, fiber, None))))
                }
            }
        }
      )

    /**
     * Removes the entry of the current fiber's entity, unless the entity was already restarted by a newer fiber
     * (e.g. its shard came back while this one was terminating).
     */
    private def removeEntry(entityId: String): UIO[Unit] =
      ZIO.fiberIdWith(fiberId =>
        entities
          .modify(map =>
            map.get(entityId) match {
              case Some(entry) if entry.fiber.id != fiberId => (false, map)
              case _                                        => (true, map - entityId)
            }
          )
          .flatMap(ZIO.when(_)(entitiesLastReceivedAt.update(_ - entityId)))
          .unit
      )

    def terminateEntitiesOnShards(shards: Set[ShardId]): UIO[Unit] =
      entities.modify { entities =>
        // get all entities on the given shards to terminate them
        entities.partition { case (entityId, _) => shards.contains(sharding.getShardId(recipientType, entityId)) }
      }
        .flatMap(terminateEntities)

    def terminateAllEntities: UIO[Unit] =
      entities.getAndSet(Map()).flatMap(terminateEntities)

    private def terminateEntities(entitiesToTerminate: Map[String, Entry[Req]]): UIO[Unit] =
      for {
        // send termination message to all entities
        terminating <- ZIO.foreach(entitiesToTerminate.values.toList) { entry =>
                         entry.termination match {
                           case Some(p) => ZIO.some(p -> entry.fiber)
                           case None    =>
                             Promise.make[Nothing, Unit].flatMap { p =>
                               terminateMessage(p) match {
                                 case Some(terminate) =>
                                   entry.queue
                                     .offer(terminate)
                                     .catchAllCause(_ => p.succeed(()))
                                     .as(Some(p -> entry.fiber))
                                 case None            =>
                                   entry.queue.shutdown.as(None)
                               }
                             }
                         }
                       }
        // wait until they are all terminated, interrupting the ones that take too long
        _           <- awaitTermination(terminating.flatten)
      } yield ()
  }
}
