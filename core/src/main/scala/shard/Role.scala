package shard

case class Role(name: String)

object Role {
  val default: Role = Role("default")
}
