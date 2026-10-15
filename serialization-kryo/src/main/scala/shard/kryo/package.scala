package shard

import shard.interfaces.MessageCodec

/**
 * Kryo backend given. Import with `import shard.kryo.given` to enable
 * Kryo-backed serialisation for all `EntityType` / `TopicType` declarations in the
 * surrounding scope.
 *
 * To use a custom Kryo configuration, shadow this given with your own:
 * {{{
 *   given [Msg[+_, +_]]: MessageCodec[Msg] = KryoMessageCodec.fromConfig[Msg](myConfig)
 * }}}
 */
package object kryo {
  given derive[Msg[+_, +_]]: MessageCodec[Msg] = KryoMessageCodec.default[Msg]
}
