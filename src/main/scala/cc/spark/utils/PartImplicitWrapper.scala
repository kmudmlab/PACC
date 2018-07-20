package cc.spark.utils

object PartImplicitWrapper {

  val COPYID_MASK = 0xFFFF00000000L
  val NODEID_MASK = 0x0000FFFFFFFFL

  implicit class CopyOps(n: Long){

    def nodeId: Long = NODEID_MASK & n
    def copyId: Long = (n & COPYID_MASK) >>> 32

    def part(p: Int): Int = n.nodeId.hashCode() % p

    def encode(p: Int): Long = (p.toLong << 32) | n.nodeId

    def tuple: (Long, Long) = (n.nodeId, n.copyId)


  }
}
