/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * Copyright (c) 2018, Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Seoul National University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * -------------------------------------------------------------------------
 * File: CopyImplicitWrapper.scala
 */

package cc.spark.utils

/** Implicit conversions and helpers for [[cc.spark.AltOpt]].
  *
  * The first bit is 1, the node is high node.
  * The second bit is 1, the node is a copy.
  * The next 10 bits are the copyid.
  * Remainder are the nodeid.
  *
  * high bit  node id (8 * 4 bits)  copy bit   copy id (10 bits)
  * ↓                ↓                 ↓        ↓
  * - -------------------------------- - ----------
  * 0 00000000000000000000000000000000 0 0000000000
  *
  */
object CopyImplicitWrapper {

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
