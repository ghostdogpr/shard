package shard

case class PodAddress(host: String, port: Int) {
  override def toString: String = s"$host:$port"
}

object PodAddress {
  def apply(s: String): Option[PodAddress] =
    s.lastIndexOf(':') match {
      case -1 => None
      case i  => s.drop(i + 1).toIntOption.map(PodAddress(s.take(i), _))
    }
}
