package shard

import zio.Scope
import zio.test._
import zio.test.TestAspect.{sequential, withLiveClock}

import shard.interfaces.MessageStorage

object MessageStorageMemorySpec extends ZIOSpecDefault {
  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("MessageStorageMemorySpec")(MessageStorageSuite.tests*).provideShared(MessageStorage.memory) @@ sequential @@
      withLiveClock
}
