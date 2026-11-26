package shard

import java.nio.charset.StandardCharsets.UTF_8
import javax.sql.DataSource

import scala.io.Source

import com.augustnagro.magnum.{connect, sql}
import com.dimafeng.testcontainers.GenericContainer
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.wait.strategy.Wait
import zio._
import zio.test._
import zio.test.TestAspect.{sequential, timeout, withLiveClock}

import shard.interfaces.MessageStorage
import shard.interfaces.MessageStorage.{DeadLetter, Key, Stored}
import shard.interfaces.Pods.BinaryReply

object MessageStoragePostgresSpec extends ZIOSpecDefault {
  val container: ZLayer[Any, Nothing, GenericContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = new GenericContainer(
            dockerImage = "postgres:19.0-alpine",
            exposedPorts = Seq(5432),
            env = Map("POSTGRES_PASSWORD" -> "shard"),
            // the server restarts once after initializing the database
            waitStrategy = Some(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
          )
          container.start()
          container
        }.orDie
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }

  val dataSource: ZLayer[GenericContainer, Throwable, DataSource] =
    ZLayer {
      for {
        container <- ZIO.service[GenericContainer]
        dataSource = new PGSimpleDataSource()
        _          = dataSource.setURL(s"jdbc:postgresql://${container.host}:${container.mappedPort(5432)}/postgres")
        _          = dataSource.setUser("postgres")
        _          = dataSource.setPassword("shard")
        schema    <-
          ZIO.attemptBlocking(scala.util.Using.resource(Source.fromResource("shard/message-storage.sql", getClass.getClassLoader))(_.mkString))
        _         <- ZIO.attemptBlocking(scala.util.Using.resource(dataSource.getConnection())(_.createStatement().execute(schema)))
      } yield dataSource
    }

  private def storage(config: PostgresConfig): ZLayer[DataSource, Nothing, MessageStorage] =
    ZLayer.service[DataSource] ++ ZLayer.succeed(config) >>> MessageStoragePostgres.live

  private def count(entityType: String): ZIO[DataSource, Throwable, Long] =
    ZIO.serviceWithZIO[DataSource](dataSource =>
      ZIO.attemptBlocking(connect(dataSource)(sql"SELECT count(*) FROM shard_messages WHERE entity_type = $entityType".query[Long].run().sum))
    )

  private val cleanup =
    test("a pod deletes the expired results in batches, and keeps the other results, the messages that have no result and the dead letters") {
      def key(deliveryKey: String) = Key("cleanup-type", 1, "id", deliveryKey)
      val expiring                 = (1 to 5).map(n => key(n.toString))
      for {
        storage <- ZIO.service[MessageStorage]
        _       <- ZIO.foreachDiscard(expiring)(key => storage.saveResult(Role.default, key, BinaryReply.Success(Array.emptyByteArray), 1.milli))
        _       <- storage.saveResult(Role.default, key("dead letter"), DeadLetter("reason"), 1.milli)
        _       <- storage.saveResult(Role.default, key("kept"), BinaryReply.Success("b".getBytes(UTF_8)), 1.hour)
        _       <- storage.store(Role.default, key("pending"), "a".getBytes(UTF_8))
        left    <- (Clock.sleep(50.millis) *> count("cleanup-type")).repeatUntil(_ <= 3)
        pending <- storage.store(Role.default, key("pending"), "b".getBytes(UTF_8))
        kept    <- storage.store(Role.default, key("kept"), "c".getBytes(UTF_8))
        dead    <- storage.deadLetters(Role.default, "cleanup-type", Set(1)).runCollect
      } yield assertTrue(
        left == 3,
        pending match {
          case Stored.Pending(body) => String(body, UTF_8) == "a"
          case _                    => false
        },
        kept match {
          case Stored.Done(BinaryReply.Success(body)) => String(body, UTF_8) == "b"
          case _                                      => false
        },
        dead.map((key, reason, _) => (key, reason)) == Chunk((key("dead letter"), "reason"))
      )
    }.provideSomeLayer[DataSource](storage(PostgresConfig(cleanupInterval = 100.millis, cleanupBatchSize = 2)))

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("MessageStoragePostgresSpec")(
      suite("contract")(MessageStorageSuite.tests*).provideSomeLayerShared[DataSource](storage(PostgresConfig.default)),
      cleanup
    ).provideLayerShared(container >>> dataSource) @@ sequential @@ withLiveClock @@ timeout(1.minute)
}
