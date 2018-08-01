package cc.spark.utils

object CopyImplicitWrapper {

  //The first bit is 1, the node is high node.
  //The second bit is 1, the node is a copy.
  //The next 10 bits are the copyid.
  //Remainder are the nodeid.


  /**
    * high bit  node id (8 * 4 bits)  copy bit   copy id (10 bits)
    * ↓                ↓                 ↓        ↓
    * - -------------------------------- - ----------
    * 0 00000000000000000000000000000000 0 0000000000
    *
    */

  val HIGH_POSITION = 43
  val NODEID_POSITION = 11
  val COPY_POSITION = 10
  val COPYID_POSITION = 0
  val COMP_POSITION = 10

  val HIGH_MASK: Long = 1L << HIGH_POSITION
  val LOW_MASK: Long = ~HIGH_MASK
  val NODEID_MASK: Long = 0xFFFFFFFFL << NODEID_POSITION
  val COPY_MASK: Long = 1L << COPY_POSITION
  val COPYID_MASK: Long = 0x3FFL << COPYID_POSITION
  val COMP_MASK: Long = NODEID_MASK | COPY_MASK



  implicit class CopyOps(n: Long){

    def copyId: Long = (n & COPYID_MASK) >>> COPYID_POSITION

    def isHigh: Boolean = (n & HIGH_MASK) != 0

    def isCopy: Boolean = (n & COPY_MASK) != 0

    def nonCopy: Boolean = (n & COPY_MASK) == 0

    def nodeId: Long = (n & NODEID_MASK) >>> NODEID_POSITION

    def copy(v: Long, p: Int) = COPY_MASK | ((v.nodeId % p) << COPYID_POSITION) | n.nodeId << NODEID_POSITION

    def high: Long = n | HIGH_MASK

    def low: Long = n & LOW_MASK

    def comp: Long = n & COMP_MASK >>> COMP_POSITION

    def hash: Int ={
      (n.nodeId + 41 * n.copyId + (if (n.isCopy) 1681 else 0)).hashCode()
    }

    def toNode: Long = n << NODEID_POSITION

    def toTuple: String = n.nodeId + (if(n.isHigh) "h" else "") + (if(n.isCopy) "c" + n.copyId else "" )
  }
}
