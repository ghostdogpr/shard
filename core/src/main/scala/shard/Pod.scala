package shard

/**
 * A pod of a role, as registered to the Shard Manager.
 * `registration` identifies one registration of the pod: each call to `Sharding.register` starts a new one, and the
 * Shard Manager's calls to the pod carry it.
 */
case class Pod(address: PodAddress, version: String, role: Role, registration: String)

object Pod {

  /**
   * How storages keeping a pod under its address store the rest of it: its version and its registration with `%` and
   * spaces escaped, each followed by a space, then its role name
   */
  private[shard] def encode(pod: Pod): String =
    s"${escape(pod.version)} ${escape(pod.registration)} ${pod.role.name}"

  private[shard] def decode(address: PodAddress, value: String): Option[Pod] =
    value.split(" ", 3) match {
      case Array(version, registration, role) =>
        Some(Pod(address, unescape(version), Role(role), unescape(registration)))
      case _                                  => None
    }

  private def escape(value: String): String   = value.replace("%", "%25").replace(" ", "%20")
  private def unescape(value: String): String = value.replace("%20", " ").replace("%25", "%")
}
