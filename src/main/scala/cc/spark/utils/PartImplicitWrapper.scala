/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: PartImplicitWrapper.scala
 */

package cc.spark.utils

/** Implicit conversions and helpers for [[cc.spark.PACC]], and [[cc.spark.PACCOpt]]. */
object PartImplicitWrapper {

  val COPYID_MASK = 0x3FFL
  val NODEID_MASK = 0xFFFFFFFFFC00L

  implicit class CopyOps(n: Long){

    def nodeId: Long = (n & NODEID_MASK) >> 10

    def copyId: Long = n & COPYID_MASK

    def part(p: Int): Int = n.hashCode() % p

    def encode(p: Int): Long = p | ((n << 10) & NODEID_MASK)

    def tuple: (Long, Long) = (n.nodeId, n.copyId)

    def mod(p: Int): Int ={
      val rawMod = n.hashCode() % p
      rawMod + (if (rawMod < 0) p else 0)
    }
  }
}
