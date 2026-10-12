package shard

import zio._
import zio.test.TestAspect.withLiveClock
import zio.test._

object GrpcPodsSpec extends ZIOSpecDefault {
  // nothing listens on that port: calls fail, but the connection is still opened and tracked
  private val pod = PodAddress("localhost", 1)

  private val makeConnections: URIO[Scope, Ref.Synchronized[Map[PodAddress, GrpcPods.Connection]]] =
    Ref.Synchronized
      .make(Map.empty[PodAddress, GrpcPods.Connection])
      .withFinalizer(_.get.flatMap(ZIO.foreachDiscard(_) { case (_, connection) => connection.fiber.interrupt }))

  private def makePods(connections: Ref.Synchronized[Map[PodAddress, GrpcPods.Connection]]): GrpcPods =
    new GrpcPods(GrpcConfig.default.copy(connectionIdleTimeout = 100.millis), connections)

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("GrpcPodsSpec")(
      test("Idle connections are closed") {
        for {
          connections <- makeConnections
          pods         = makePods(connections)
          _           <- pods.ping(pod).ignore
          before      <- connections.get
          _           <- Clock.sleep(200.millis)
          _           <- pods.closeIdleConnections
          after       <- connections.get
        } yield assertTrue(before.contains(pod), after.isEmpty)
      },
      test("Connections in use are not closed") {
        for {
          connections <- makeConnections
          pods         = makePods(connections)
          _           <- pods.ping(pod).ignore
          connection  <- connections.get.map(_(pod))
          _           <- connection.inFlight.set(1)
          _           <- Clock.sleep(200.millis)
          _           <- pods.closeIdleConnections
          after       <- connections.get
        } yield assertTrue(after.get(pod).contains(connection))
      },
      test("A new connection is opened after an idle one was closed") {
        for {
          connections <- makeConnections
          pods         = makePods(connections)
          _           <- pods.ping(pod).ignore
          first       <- connections.get.map(_(pod))
          _           <- Clock.sleep(200.millis)
          _           <- pods.closeIdleConnections
          _           <- pods.ping(pod).ignore
          second      <- connections.get.map(_.get(pod))
        } yield assertTrue(second.exists(_.fiber.id != first.fiber.id))
      }
    ) @@ withLiveClock @@ TestAspect.timeout(20.seconds)
}
