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
 * File: StarGroupOps.scala
 * - Implicit conversions and helpers for [[cc.spark.PACC]], [[cc.spark.PACCOpt]], [[cc.spark.PACCBase]],
 *   [[cc.spark.AltOpt]], and [[cc.spark.AltOpt]].
 */


package cc.spark.utils

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

/**
  * Implicit conversions and helpers for [[cc.spark.PACC]], [[cc.spark.PACCOpt]], [[cc.spark.PACCBase]],
  * [[cc.spark.AltOpt]], and [[cc.spark.AltOpt]].
  */
object StarGroupOps {
  implicit class StarRDDOp(rdd: RDD[(Long, Long)]){
    def starGrouped(partitioner: Partitioner = new HashPartitioner(rdd.getNumPartitions)): RDD[(Long, Iterator[Long])] = {

      val hdconf = rdd.sparkContext.hadoopConfiguration
      val tmpPaths = hdconf.getTrimmedStrings("yarn.nodemanager.local-dirs")

      rdd.partitionBy(new HashPartitioner(rdd.getNumPartitions))
        .mapPartitions { it =>
          new PairExternalSorter(tmpPaths).sort(it).starGrouped()
        }

    }
  }

  implicit class StarIteratorOp(it: Iterator[(Long, Long)]){

    /**
      * It returns an RDD where pairs are grouped by key.
      *
      * @return an RDD where pairs are grouped by key.
      */
    def starGrouped()= new Iterator[(Long, Iterator[Long])] {
      var first: Option[(Long, Long)] = None
      var prev: GroupedIterator = _

      override def hasNext: Boolean = first.isDefined ||
        (prev != null && {
          first = prev.consumeAndGetHead
          first.isDefined
        }) ||
        (it.hasNext && {
          first = Some(it.next())
          first.isDefined
        })

      override def next(): (Long, Iterator[Long]) = {

        if(hasNext) {
          prev = new GroupedIterator(first, it)
          val res = (first.get._1, prev.map(_._2))
          first = None
          res
        }
        else Iterator.empty.next()

      }

      class GroupedIterator(first: Option[(Long, Long)], base: Iterator[(Long, Long)]) extends Iterator[(Long, Long)]{

        private var (hd, hdDefined): ((Long, Long), Boolean) = first match {
          case Some(x) => (x, true)
          case None => (null, false)
        }
        var tailConsumed: Boolean = false

        private var tail: Iterator[(Long, Long)] = base

        def hasNext = hdDefined || tail.hasNext && {

          val cur = tail.next()
          tailConsumed = true

          if(cur._1 == hd._1) hdDefined = true
          else tail = Iterator.empty

          hd = cur
          hdDefined

        }
        def next() = if (hasNext) { hdDefined = false; tailConsumed = false; hd } else Iterator.empty.next()

        def consumeAndGetHead: Option[(Long, Long)] = {
          while(hasNext) next

          if(tailConsumed) Some(hd)
          else None

        }
      }
    }
  }
}
