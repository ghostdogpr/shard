package example

import sage.{ Endpoint, SageConfig, SageException, Topology }
import sage.backend.SageClient
import zio.ZLayer

package object complex {
  val redis: ZLayer[Any, SageException, SageClient] =
    SageClient.layer(SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379))))
}
