package shard

case class Pod(address: PodAddress, version: String, role: Role)

object Pod {

  /**
   * How storages keeping a pod under its address store the rest of it: its version with `%` and spaces escaped, a space, then its role name
   */
  private[shard] def encode(pod: Pod): String =
    s"${pod.version.replace("%", "%25").replace(" ", "%20")} ${pod.role.name}"

  private[shard] def decode(address: PodAddress, value: String): Option[Pod] =
    value.split(" ", 2) match {
      case Array(version, role) => Some(Pod(address, version.replace("%20", " ").replace("%25", "%"), Role(role)))
      case _                    => None
    }
}
