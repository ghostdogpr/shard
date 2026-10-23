package shard.protocol

import proteus._
import shard.interfaces.Pods.{ BinaryMessage, BinaryReply }

object Sharding {
  given ProtobufDeriver =
    ProtobufDeriver.modifier[BinaryReply](Modifiers.oneOf(Modifiers.OneOfFlag.Inline, Modifiers.OneOfFlag.Nested))

  case class AssignShardsRequest(shards: List[Int]) derives ProtobufCodec
  case class AssignShardsResponse() derives ProtobufCodec

  case class UnassignShardsRequest(shards: List[Int]) derives ProtobufCodec
  case class UnassignShardsResponse() derives ProtobufCodec

  given ProtobufCodec[BinaryMessage] = ProtobufCodec.derived
  given ProtobufCodec[BinaryReply]   = ProtobufCodec.derived
  case class SendResponse(reply: BinaryReply) derives ProtobufCodec

  case class PingShardsRequest() derives ProtobufCodec
  case class PingShardsResponse() derives ProtobufCodec

  val assignShards               = Rpc.unary[AssignShardsRequest, AssignShardsResponse]("AssignShards")
  val unassignShards             = Rpc.unary[UnassignShardsRequest, UnassignShardsResponse]("UnassignShards")
  val send                       = Rpc.unary[BinaryMessage, SendResponse]("Send")
  val sendStream                 = Rpc.clientStreaming[BinaryMessage, SendResponse]("SendStream")
  val sendStreamAndReceiveStream = Rpc.bidiStreaming[BinaryMessage, SendResponse]("SendStreamAndReceiveStream")
  val pingShards                 = Rpc.unary[PingShardsRequest, PingShardsResponse]("PingShards")

  val service =
    Service("ShardingService")
      .rpc(assignShards)
      .rpc(unassignShards)
      .rpc(send)
      .rpc(sendStream)
      .rpc(sendStreamAndReceiveStream)
      .rpc(pingShards)
}

object GenerateProto {
  def main(args: Array[String]): Unit = {
    val outputDir = args.headOption.getOrElse {
      sys.error("Usage: GenerateProto <output-folder>")
    }
    Sharding.service.renderToFile(List.empty, outputDir, "sharding")
    println(s"Wrote $outputDir/sharding.proto")
    ShardManager.service.renderToFile(List.empty, outputDir, "shard_manager")
    println(s"Wrote $outputDir/shard_manager.proto")
  }
}
