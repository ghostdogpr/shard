package shard.protocol

import shard.{ Pod, PodAddress, Role, ShardId, ShardingEvent }
import proteus._

object ShardManager {
  case class Assignment(shardId: ShardId, pod: Option[PodAddress])

  given ProtobufDeriver = ProtobufDeriver.instance[Map[ShardId, Option[PodAddress]]](
    ProtobufCodec
      .derived[List[Assignment]](using ProtobufDeriver)
      .transform(_.map(a => a.shardId -> a.pod).toMap, _.map(Assignment(_, _)).toList)
  )

  given ProtobufCodec[ShardingEvent] = ProtobufCodec.derived

  case class GetAssignmentsRequest(role: Role) derives ProtobufCodec
  case class GetAssignmentsResponse(assignments: Map[ShardId, Option[PodAddress]]) derives ProtobufCodec

  case class RegisterRequest(pod: Pod) derives ProtobufCodec
  case class RegisterResponse() derives ProtobufCodec

  case class UnregisterRequest(podAddress: PodAddress) derives ProtobufCodec
  case class UnregisterResponse() derives ProtobufCodec

  case class NotifyUnhealthyPodRequest(podAddress: PodAddress) derives ProtobufCodec
  case class NotifyUnhealthyPodResponse() derives ProtobufCodec

  case class CheckAllPodsHealthRequest() derives ProtobufCodec
  case class CheckAllPodsHealthResponse() derives ProtobufCodec

  case class GetShardingEventsRequest() derives ProtobufCodec

  val getAssignments     = Rpc.unary[GetAssignmentsRequest, GetAssignmentsResponse]("GetAssignments")
  val register           = Rpc.unary[RegisterRequest, RegisterResponse]("Register")
  val unregister         = Rpc.unary[UnregisterRequest, UnregisterResponse]("Unregister")
  val notifyUnhealthyPod = Rpc.unary[NotifyUnhealthyPodRequest, NotifyUnhealthyPodResponse]("NotifyUnhealthyPod")
  val checkAllPodsHealth = Rpc.unary[CheckAllPodsHealthRequest, CheckAllPodsHealthResponse]("CheckAllPodsHealth")
  val getShardingEvents  = Rpc.serverStreaming[GetShardingEventsRequest, ShardingEvent]("GetShardingEvents")

  val service =
    Service("ShardManagerService")
      .rpc(getAssignments)
      .rpc(register)
      .rpc(unregister)
      .rpc(notifyUnhealthyPod)
      .rpc(checkAllPodsHealth)
      .rpc(getShardingEvents)
}
