package shard

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import shard.interfaces.MessageCodec
import shard.interfaces.MessageCodec.Codec

/**
 * Java-serialization backend given.
 *
 * Import with `import shard.javaSerialization.given` to enable
 * Java-serialization-backed transport for all `EntityType` / `TopicType` declarations
 * in the surrounding scope.
 *
 * Mostly useful for tests and examples. For production deployments prefer
 * `shard.kryo` or a macro-derived backend.
 */
package object javaSerialization {
  given derive[Msg[+_, +_]]: MessageCodec[Msg] = MessageCodec.uniform(Codec(writeBytes, readBytes))

  private def writeBytes(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos  = new ObjectOutputStream(baos)
    try oos.writeObject(value)
    finally oos.close()
    baos.toByteArray
  }

  private def readBytes(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try ois.readObject()
    finally ois.close()
  }
}
