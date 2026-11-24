package shard

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import scala.util.Try

import sage.SageException.{DecodeError, ServerError}
import sage.backend.SageClient
import sage.commands.{as, asArray, Commands, Limit, ScoreBoundary, ZRange}
import sage.protocol.Frame
import zio.{Chunk, Duration, IO, Task, ZIO, ZLayer}
import zio.stream.ZStream

import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{DeadLetter, Key, Result, Stored}
import shard.interfaces.Pods.BinaryReply

object MessageStorageRedis {

  /**
    * A layer that returns a MessageStorage implementation using Redis.
    */
  val live: ZLayer[SageClient & RedisConfig, Nothing, MessageStorage] =
    ZLayer {
      for {
        config <- ZIO.service[RedisConfig]
        client <- ZIO.service[SageClient]
      } yield new MessageStorageRedis(client, config.messagesKey)
    }

  final private class Script(code: String) {
    val body: String = s"local message, pending, deadLetters, order, member = KEYS[1], KEYS[2], KEYS[3], KEYS[4], ARGV[1]\n$code"
    val sha: String  = MessageDigest.getInstance("SHA-1").digest(body.getBytes(UTF_8)).map("%02x".format(_)).mkString
  }

  /**
    * Store a message unless its key holds one, and return what its key holds.
    */
  private val storeScript = Script(
    """local state = redis.call('HGET', message, 'state')
      |if state then
      |  return {state, redis.call('HGET', message, state == 'pending' and 'body' or 'result')}
      |end
      |redis.call('HSET', message, 'state', 'pending', 'body', ARGV[2])
      |redis.call('ZADD', pending, redis.call('INCR', order), member)
      |return {'new', ''}
      |""".stripMargin
  )

  /**
    * Save the result of a message unless its key has one, and return the result of its key.
    */
  private val saveResultScript = Script(
    """local newState, result, retention = ARGV[2], ARGV[3], ARGV[4]
      |local state = redis.call('HGET', message, 'state')
      |if state and state ~= 'pending' then
      |  return {state, redis.call('HGET', message, 'result')}
      |end
      |redis.call('ZREM', pending, member)
      |redis.call('HSET', message, 'state', newState, 'result', result)
      |if newState == 'dead-letter' then
      |  redis.call('HSETNX', message, 'body', '')
      |  redis.call('ZADD', deadLetters, redis.call('INCR', order), member)
      |else
      |  redis.call('HDEL', message, 'body')
      |  redis.call('PEXPIRE', message, retention)
      |end
      |return {newState, result}
      |""".stripMargin
  )

  /**
    * Remove a message that has no result, or a dead letter.
    */
  private val removeScript = Script(
    """local state = redis.call('HGET', message, 'state')
      |if state == 'pending' or state == 'dead-letter' then
      |  redis.call('DEL', message)
      |  redis.call('ZREM', pending, member)
      |  redis.call('ZREM', deadLetters, member)
      |end
      |""".stripMargin
  )

  private val pageSize = 100

  // Redis rejects an expiry past the largest time it can represent
  private val maxRetentionMillis = Long.MaxValue / 2

  private def encode(value: String): String = URLEncoder.encode(value, UTF_8)

  private def encodeResult(result: Result): (String, Array[Byte]) =
    result match {
      case BinaryReply.Success(body) => ("success", body)
      case BinaryReply.Failure(body) => ("failure", body)
      case DeadLetter(reason)        => ("dead-letter", reason.getBytes(UTF_8))
    }

  private def decodeResult(state: String, payload: Array[Byte]): IO[DecodeError, Result] =
    state match {
      case "success"     => ZIO.succeed(BinaryReply.Success(payload))
      case "failure"     => ZIO.succeed(BinaryReply.Failure(payload))
      case "dead-letter" => ZIO.succeed(DeadLetter(String(payload, UTF_8)))
      case other         => ZIO.fail(DecodeError("a result state", other))
    }

  private def decodeReply(frame: Frame): IO[DecodeError, (String, Array[Byte])] =
    ZIO.fromEither(frame.asArray.flatMap {
      case Vector(state, payload) => state.as[String].flatMap(state => payload.as[Array[Byte]].map((state, _)))
      case other                  => Left(DecodeError("a state and a payload", s"${other.size} elements"))
    })
}

final private class MessageStorageRedis(client: SageClient, messagesKey: Role => String) extends MessageStorage {
  import MessageStorageRedis._

  final private class ShardKeys(role: Role, entityType: String, shardId: ShardId) {
    private val base = s"${messagesKey(role)}:{${encode(role.name)}:${encode(entityType)}:$shardId}"

    def message(member: String): String = s"$base:message:$member"

    val order: String = s"$base:order"

    // scored by `order`, so in the order in which they were stored
    val pending: String     = s"$base:pending"
    val deadLetters: String = s"$base:dead-letters"

    def key(member: String): Option[Key] =
      member.split(":", 2) match {
        case Array(entityId, deliveryKey) =>
          Try(Key(entityType, shardId, URLDecoder.decode(entityId, UTF_8), URLDecoder.decode(deliveryKey, UTF_8))).toOption
        case _                            => None
      }
  }

  private def run(script: Script, role: Role, key: Key, args: Array[Byte]*): Task[Frame] = {
    val keys       = ShardKeys(role, key.entityType, key.shardId)
    val member     = s"${encode(key.entityId)}:${encode(key.deliveryKey)}"
    val scriptKeys = Seq(keys.message(member), keys.pending, keys.deadLetters, keys.order)
    val scriptArgs = member.getBytes(UTF_8) +: args
    client.evalSha(script.sha, scriptKeys, scriptArgs).catchSome { case ServerError("NOSCRIPT", _) =>
      client.eval(script.body, scriptKeys, scriptArgs)
    }
  }

  def store(role: Role, key: Key, body: Array[Byte]): Task[Stored] =
    run(storeScript, role, key, body).flatMap(decodeReply).flatMap {
      case ("new", _)          => ZIO.succeed(Stored.New)
      case ("pending", stored) => ZIO.succeed(Stored.Pending(stored))
      case (state, payload)    => decodeResult(state, payload).map(Stored.Done(_))
    }

  def saveResult(role: Role, key: Key, result: Result, retention: Duration): Task[Result] = {
    val (state, payload) = encodeResult(result)
    val retentionMillis  = retention.toMillis.min(maxRetentionMillis).toString
    run(saveResultScript, role, key, state.getBytes(UTF_8), payload, retentionMillis.getBytes(UTF_8)).flatMap(decodeReply).flatMap(decodeResult)
  }

  def remove(role: Role, key: Key): Task[Unit] =
    run(removeScript, role, key).unit

  def pending(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, Array[Byte])] =
    ZStream.fromIterable(shards).flatMap { shardId =>
      val keys = ShardKeys(role, entityType, shardId)
      messages(keys, keys.pending, "body").collect { case (key, "pending", Vector(Some(body))) => (key, body) }
    }

  def deadLetters(role: Role, entityType: String, shards: Set[ShardId]): ZStream[Any, Throwable, (Key, String, Array[Byte])] =
    ZStream.fromIterable(shards).flatMap { shardId =>
      val keys = ShardKeys(role, entityType, shardId)
      messages(keys, keys.deadLetters, "result", "body").collect { case (key, "dead-letter", Vector(Some(reason), Some(body))) =>
        (key, String(reason, UTF_8), body)
      }
    }

  private def messages(keys: ShardKeys, set: String, fields: String*): ZStream[Any, Throwable, (Key, String, Vector[Option[Array[Byte]]])] =
    ZStream.fromIterableZIO(client.get[Long](keys.order).map(_.toList)).flatMap { last =>
      ZStream.paginateChunkZIO[Any, Throwable, (Key, String, Vector[Option[Array[Byte]]]), ScoreBoundary](ScoreBoundary.NegInf) { from =>
        for {
          page   <- client.zRangeWithScores[String](set, ZRange.ByScore(from, ScoreBoundary.Inclusive(last.toDouble), Some(Limit(0, pageSize))))
          values <- client.pipeline(page.map((member, _) => Commands.hmGet[String, String, Array[Byte]](keys.message(member), "state", fields*)))
        } yield {
          val found = page.zip(values).flatMap {
            case ((member, _), Some(state) +: rest) => keys.key(member).map((_, String(state, UTF_8), rest))
            case _                                  => None
          }
          (Chunk.fromIterable(found), page.lastOption.filter(_ => page.size == pageSize).map((_, score) => ScoreBoundary.Exclusive(score)))
        }
      }
    }
}
