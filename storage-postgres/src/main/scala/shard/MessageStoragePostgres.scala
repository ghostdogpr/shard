package shard

import java.nio.charset.StandardCharsets.UTF_8
import javax.sql.DataSource

import com.augustnagro.magnum.{sql, DbCodec, DbCon, Frag, SqlLiteral}
import com.augustnagro.magnum.magzio.TransactorZIO
import com.augustnagro.magnum.pg.PgCodec.VectorCodec
import zio.{Chunk, Duration, Task, UIO, ZIO, ZLayer}
import zio.stream.ZStream

import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{DeadLetter, Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply

object MessageStoragePostgres {

  /**
    * A layer that returns a MessageStorage implementation using Postgres, which deletes expired results in the background.
    */
  val live: ZLayer[DataSource & PostgresConfig, Nothing, MessageStorage] =
    TransactorZIO.layer ++ ZLayer.service[PostgresConfig] >>> ZLayer.scoped {
      for {
        transactor <- ZIO.service[TransactorZIO]
        config     <- ZIO.service[PostgresConfig]
        storage     = new MessageStoragePostgres(transactor, config)
        _          <- storage.cleanup.forkScoped
      } yield storage
    }

  private val pageSize = 100

  // Postgres rejects a time after the year 294276
  private val maxRetentionMillis = 100_000L * 365 * 24 * 60 * 60 * 1000

  private type Row = (String, Array[Byte], Option[Array[Byte]])

  private def encodeResult(result: Result): (String, Array[Byte]) =
    result match {
      case BinaryReply.Success(body) => ("success", body)
      case BinaryReply.Failure(body) => ("failure", body)
      case DeadLetter(reason)        => ("dead_letter", reason.getBytes(UTF_8))
    }

  private def decode(row: Row): Either[Throwable, Stored] =
    row match {
      case ("pending", body, _)             => Right(Stored.Pending(body))
      case ("success", _, Some(result))     => Right(Stored.Done(BinaryReply.Success(result)))
      case ("failure", _, Some(result))     => Right(Stored.Done(BinaryReply.Failure(result)))
      case ("dead_letter", _, Some(reason)) => Right(Stored.Done(DeadLetter(String(reason, UTF_8))))
      case (state, _, _)                    => Left(new IllegalStateException(s"A stored message has an unexpected state: $state"))
    }
}

final private class MessageStoragePostgres(transactor: TransactorZIO, config: PostgresConfig) extends MessageStorage {
  import MessageStoragePostgres._

  private val table = SqlLiteral(config.table)

  private def isKey(role: Role, key: Key): Frag =
    sql"""role = ${role.name} AND entity_type = ${key.entityType} AND shard_id = ${key.shardId}
         AND entity_id = ${key.entityId} AND delivery_key = ${key.deliveryKey}"""

  // the upsert locks the row it conflicts with even when it doesn't update it, so no other call can change that row before this reads it
  private def conflicting(role: Role, key: Key)(using DbCon): Either[Throwable, Stored] =
    sql"SELECT state, body, result FROM $table WHERE ${isKey(role, key)}"
      .query[Row]
      .run()
      .headOption
      .toRight(new IllegalStateException("A stored message is missing after its key was locked"))
      .flatMap(decode)

  def store(role: Role, key: Key, body: Array[Byte]): Task[Stored] =
    transactor
      .transact {
        val inserted =
          sql"""INSERT INTO $table AS m (role, entity_type, shard_id, entity_id, delivery_key, state, body)
              VALUES (${role.name}, ${key.entityType}, ${key.shardId}, ${key.entityId}, ${key.deliveryKey}, 'pending', $body)
              ON CONFLICT (role, entity_type, shard_id, entity_id, delivery_key) DO UPDATE
              SET state = 'pending', body = EXCLUDED.body, result = NULL, expires_at = NULL, position = DEFAULT
              WHERE m.expires_at <= now()""".update.run()
        if (inserted == 1) Right(Stored.New) else conflicting(role, key)
      }
      .flatMap(ZIO.fromEither(_))

  def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result] = {
    val (state, payload) = encodeResult(result)
    val retentionMillis  = retention.toMillis.min(maxRetentionMillis)
    transactor
      .transact {
        val saved =
          sql"""INSERT INTO $table AS m (role, entity_type, shard_id, entity_id, delivery_key, state, body, result, expires_at)
              VALUES (${role.name}, ${key.entityType}, ${key.shardId}, ${key.entityId}, ${key.deliveryKey}, $state, ''::bytea, $payload,
                      CASE WHEN $state = 'dead_letter' THEN NULL ELSE now() + CAST($retentionMillis AS bigint) * interval '1 millisecond' END)
              ON CONFLICT (role, entity_type, shard_id, entity_id, delivery_key) DO UPDATE
              SET state = EXCLUDED.state, result = EXCLUDED.result, expires_at = EXCLUDED.expires_at,
                  body = CASE WHEN EXCLUDED.state = 'dead_letter' AND m.state = 'pending' THEN m.body ELSE ''::bytea END
              WHERE m.state = 'pending' OR m.expires_at <= now()""".update.run()
        if (saved == 1) Right(result)
        else
          conflicting(role, key).flatMap {
            case Stored.Done(kept) => Right(kept)
            case other             => Left(new IllegalStateException(s"A message has no result after it was saved: $other"))
          }
      }
      .flatMap(ZIO.fromEither(_))
  }

  def remove(role: Role, key: Key): Task[Unit] =
    transactor.transact(sql"DELETE FROM $table WHERE ${isKey(role, key)} AND state IN ('pending', 'dead_letter')".update.run()).unit

  def pending(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, Array[Byte])] =
    paged[Array[Byte], (Key, Array[Byte])](role, entityType, "pending", shards, sql"body")((_, _))

  def deadLetters(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, String, Array[Byte])] =
    paged[(Array[Byte], Array[Byte]), (Key, String, Array[Byte])](role, entityType, "dead_letter", shards, sql"result, body") {
      case (key, (reason, body)) => (key, String(reason, UTF_8), body)
    }

  private def paged[P: DbCodec, A](role: Role, entityType: String, state: String, shards: Set[ShardId], payload: Frag)(
    decode: (Key, P) => A
  ): ZStream[Any, Throwable, A] = {
    val shardIds = shards.toVector
    val rows     = sql"role = ${role.name} AND entity_type = $entityType AND state = $state"
    // the max of each shard is one index lookup, while the max of several shards reads all their rows
    val last     = sql"""SELECT coalesce(max(last), 0) FROM unnest($shardIds) AS s(id),
                         LATERAL (SELECT max(position) AS last FROM $table WHERE $rows AND shard_id = s.id) AS l"""
    ZStream
      .fromZIO(transactor.transact(last.query[Long].run().headOption.getOrElse(0L)))
      .flatMap(last =>
        ZStream.paginateChunkZIO((Int.MinValue, Long.MinValue)) { (shardId, position) =>
          transactor.transact {
            val page = sql"""SELECT shard_id, position, entity_id, delivery_key, $payload FROM $table
                             WHERE $rows AND shard_id = ANY($shardIds) AND (shard_id, position) > ($shardId, $position) AND position <= $last
                             ORDER BY shard_id, position LIMIT $pageSize""".query[(ShardId, Long, String, String, P)].run()
            (
              Chunk.fromIterable(page).map((shardId, _, entityId, deliveryKey, p) => decode(Key(entityType, shardId, entityId, deliveryKey), p)),
              page.lastOption.filter(_ => page.size == pageSize).map((shardId, position, _, _, _) => (shardId, position))
            )
          }
        }
      )
  }

  private val deleteExpired: Task[Int] =
    transactor.transact {
      // materialized so that the locking subquery runs once, and the batch has at most its size
      sql"""WITH expired AS MATERIALIZED (
              SELECT ctid FROM $table WHERE expires_at <= now() LIMIT ${config.cleanupBatchSize} FOR UPDATE SKIP LOCKED
            )
            DELETE FROM $table WHERE ctid IN (SELECT ctid FROM expired)""".update.run()
    }

  val cleanup: UIO[Nothing] =
    (deleteExpired
      .repeatWhile(_ == config.cleanupBatchSize)
      .catchAllCause(ZIO.logWarningCause("Failed to delete the expired results of AtLeastOnce messages", _)) *>
      ZIO.sleep(config.cleanupInterval)).forever
}
