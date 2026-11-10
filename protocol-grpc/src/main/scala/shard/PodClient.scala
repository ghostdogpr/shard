package shard

import shard.protocol.Sharding
import shard.protocol.Sharding._
import shard.interfaces.Pods.BinaryMessage
import io.grpc.StatusException
import proteus.client.ZioClientBackend
import zio._
import zio.stream.ZStream

/**
 * Bundles the per-RPC client functions for a single pod. The underlying channel's
 * lifecycle is managed by the scope that builds the backend.
 */
private[shard] final class PodClient(backend: ZioClientBackend) {
  val assignShards: AssignShardsRequest => IO[StatusException, AssignShardsResponse]                                  =
    backend.client(Sharding.assignShards, Sharding.service)
  val unassignShards: UnassignShardsRequest => IO[StatusException, UnassignShardsResponse]                            =
    backend.client(Sharding.unassignShards, Sharding.service)
  val pingShards: PingShardsRequest => IO[StatusException, PingShardsResponse]                                        =
    backend.client(Sharding.pingShards, Sharding.service)
  val send: BinaryMessage => IO[StatusException, SendResponse]                                                        =
    backend.client(Sharding.send, Sharding.service)
  val sendStream: ZStream[Any, Nothing, BinaryMessage] => IO[StatusException, SendResponse]                           =
    backend.client(Sharding.sendStream, Sharding.service)
  val sendStreamAndReceiveStream: ZStream[Any, Nothing, BinaryMessage] => ZStream[Any, StatusException, SendResponse] =
    backend.client(Sharding.sendStreamAndReceiveStream, Sharding.service)
}
