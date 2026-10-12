import scala.collection.mutable

package object shard {
  type ShardId     = Int
  type EpochMillis = Long

  private[shard] def renderShardIds(ids: Iterable[ShardId]): String =
    ids
      .foldLeft(mutable.BitSet.empty)(_ += _)
      .mkString("[", ", ", "]")

}
