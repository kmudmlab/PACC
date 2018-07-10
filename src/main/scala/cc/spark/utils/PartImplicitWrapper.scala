package cc.spark.utils

object PartImplicitWrapper {

  //The first bit is 1, the node is high node.
  //The second bit is 1, the node is a copy.
  //The next 10 bits are the copyid.
  //Remainder are the nodeid.

  val COPYID_MASK = 0xFFFF00000000L
  val NODEID_MASK = 0x0000FFFFFFFFL

  implicit class CopyOps(n: Long){

    def nodeId: Long = NODEID_MASK & n
    def copyId: Long = (n & COPYID_MASK) >>> 32

    def part(v: Long, p: Int) = ((v.nodeId % p) << 32) | n.nodeId

  }
}
