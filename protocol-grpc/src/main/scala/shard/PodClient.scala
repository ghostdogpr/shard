package shard

import shard.protocol.Sharding
import shard.protocol.Sharding._
import shard.interfaces.Pods.BinaryMessage
import io.grpc.StatusException
import proteus.client.ZioClientBackend
import zio._
import zio.stream.ZStream

/**
 * Bundles the per-RPC client functions for a single pod. The functions are thin
 * wrappers around the proteus backend; the underlying channel's lifecycle is
 * managed by the scope that builds the backend.
 */
private[shard] trait PodClient {
  def assignShards(request: AssignShardsRequest): IO[StatusException, AssignShardsResponse]
  def unassignShards(request: UnassignShardsRequest): IO[StatusException, UnassignShardsResponse]
  def pingShards(request: PingShardsRequest): IO[StatusException, PingShardsResponse]
  def send(request: BinaryMessage): IO[StatusException, SendResponse]
  def sendStream(requests: ZStream[Any, StatusException, BinaryMessage]): IO[StatusException, SendResponse]
  def sendStreamAndReceiveStream(
    requests: ZStream[Any, StatusException, BinaryMessage]
  ): ZStream[Any, StatusException, SendResponse]
}

private[shard] object PodClient {
  def from(backend: ZioClientBackend): PodClient =
    new PodClient {
      private val assignShardsCall               = backend.client(Sharding.assignShards, Sharding.service)
      private val unassignShardsCall             = backend.client(Sharding.unassignShards, Sharding.service)
      private val pingShardsCall                 = backend.client(Sharding.pingShards, Sharding.service)
      private val sendCall                       = backend.client(Sharding.send, Sharding.service)
      private val sendStreamCall                 = backend.client(Sharding.sendStream, Sharding.service)
      private val sendStreamAndReceiveStreamCall =
        backend.client(Sharding.sendStreamAndReceiveStream, Sharding.service)

      def assignShards(request: AssignShardsRequest)                                         = assignShardsCall(request)
      def unassignShards(request: UnassignShardsRequest)                                     = unassignShardsCall(request)
      def pingShards(request: PingShardsRequest)                                             = pingShardsCall(request)
      def send(request: BinaryMessage)                                                       = sendCall(request)
      def sendStream(requests: ZStream[Any, StatusException, BinaryMessage])                 = sendStreamCall(requests)
      def sendStreamAndReceiveStream(requests: ZStream[Any, StatusException, BinaryMessage]) =
        sendStreamAndReceiveStreamCall(requests)
    }
}
