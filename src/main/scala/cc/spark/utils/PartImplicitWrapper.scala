package cc.spark.utils

object PartImplicitWrapper {

  val COPYID_MASK = 0xFFFFL
  val NODEID_MASK = 0xFFFFFFFF0000L

  implicit class CopyOps(n: Long){

    def nodeId: Long = (n & NODEID_MASK) >> 16
    def copyId: Long = n & COPYID_MASK

    def part(p: Int): Int = n.nodeId.hashCode() % p

    def encode(p: Int): Long = p | ((n << 16) & NODEID_MASK)

    def tuple: (Long, Long) = (n.nodeId, n.copyId)

    def mod(p: Int): Int ={
      val rawMod = n.hashCode() % p
      rawMod + (if (rawMod < 0) p else 0)
    }
  }
}
