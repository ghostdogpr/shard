package shard.kryo

import shard.interfaces.MessageCodec
import shard.interfaces.MessageCodec.Codec
import com.typesafe.config.{ Config, ConfigFactory }
import io.altoo.serialization.kryo.scala.ScalaKryoSerializer

/**
 * Universal `MessageCodec[Msg]`s backed by Kryo via altoo's `ScalaKryoSerializer`.
 *
 * Because Kryo is reflective and uniform across types, one generic codec serves every
 * message type — there is no per-type derivation.
 * Users typically import the package-level given via `import shard.kryo.given`
 * and define their `EntityType` / `TopicType` without further ceremony.
 *
 * To use a custom Kryo configuration, shadow the default `given` with
 * `given [Msg[+_, +_]]: MessageCodec[Msg] = KryoMessageCodec.fromConfig[Msg](myConfig)`.
 */
object KryoMessageCodec {

  /**
   * Shared `ScalaKryoSerializer` built from Typesafe Config's reference configuration.
   * The serializer manages a Kryo pool internally and is safe to share across the whole pod.
   */
  lazy val defaultSerializer: ScalaKryoSerializer =
    new ScalaKryoSerializer(ConfigFactory.defaultReference(), getClass.getClassLoader)

  /**
   * Create a Kryo codec that uses [[defaultSerializer]].
   */
  def default[Msg[+_, +_]]: MessageCodec[Msg] = fromSerializer(defaultSerializer)

  /**
   * Create a Kryo codec from the given Typesafe Config.
   * See https://github.com/altoo-ag/scala-kryo-serialization for the available options.
   */
  def fromConfig[Msg[+_, +_]](config: Config): MessageCodec[Msg] =
    fromSerializer(new ScalaKryoSerializer(config, getClass.getClassLoader))

  private def fromSerializer[Msg[+_, +_]](serializer: ScalaKryoSerializer): MessageCodec[Msg] =
    MessageCodec.uniform(
      Codec(value => serializer.serialize(value).get, bytes => serializer.deserialize[Any](bytes).get)
    )
}
