package shard

import shard.interfaces.MessageCodec

/**
 * An abstract type to extend for each type of entity or topic.
 * Each variant of `Msg[E, A]` declares its error type `E` and reply type `A`:
 * {{{
 *   enum GuildMessage[+E, +A] {
 *     case Join(userId: String)  extends GuildMessage[GuildFull, Set[String]]
 *     case Leave(userId: String) extends GuildMessage[Nothing, Unit]
 *   }
 *   object Guild extends EntityType[GuildMessage]("guild")
 * }}}
 *
 * The `MessageCodec[Msg]` carried via the implicit parameter owns serialisation for this
 * recipient type. Users provide a codec by importing one of the backend `given`s in scope
 * at the declaration site, e.g. `import shard.kryo.given`.
 *
 * @param name a unique string that identifies this entity or topic type
 * @tparam Msg the type of message that can be sent to this entity or topic type
 */
sealed abstract class RecipientType[Msg[+_, +_]](val name: String)(using private[shard] val codec: MessageCodec[Msg]) {

  /**
   * The handler that processes the messages of this entity or topic type
   */
  type Handler = EntityHandler[Msg]

  def getShardId(entityId: String, numberOfShards: Int): ShardId =
    math.abs(entityId.hashCode % numberOfShards) + 1
}
abstract class EntityType[Msg[+_, +_]](name: String)(using MessageCodec[Msg]) extends RecipientType[Msg](name)
abstract class TopicType[Msg[+_, +_]](name: String)(using MessageCodec[Msg])  extends RecipientType[Msg](name)
