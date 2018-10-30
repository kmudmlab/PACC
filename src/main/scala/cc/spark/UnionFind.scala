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
 * File: UnionFind.scala
 * - The spark version of UnionFind. It finds connected components in a graph.
 */

package cc.spark

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectIterator
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/** UnionFind for finding connected components.
  * Call [[UnionFind.run()]] to run.
  */
object UnionFind {

  /**
    * Find connected components.
    *
    * @param edges the edges of an input graph
    * @return iterator of connected components
    */
  def run(edges: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {

    val parent = new Long2LongOpenHashMap
    parent.defaultReturnValue(-1)

    // find the minimum node rechable from node x.
    def find_root(x: Long): Long = {
      val p = parent.get(x)
      if (p != -1) {
        val new_p = find_root(p)
        parent.put(x, new_p)
        new_p
      }
      else x
    }

    // union two connected components concerning the nodes in edge (x, y)
    def union(x: Long, y: Long): Unit = {
      val r1 = find_root(x)
      val r2 = find_root(y)

      if (r1 > r2)
        parent.put(r1, r2)
      else if (r1 < r2)
        parent.put(r2, r1)
    }

    for ((u, v) <- edges) {
      if (find_root(u) != find_root(v)) {
        union(u, v)
      }
    }

    new Iterator[(Long, Long)] {

      val it = parent.long2LongEntrySet.fastIterator

      override def hasNext: Boolean = it.hasNext

      override def next(): (Long, Long) = {
        val pair = it.next()
        (pair.getLongKey, find_root(pair.getLongValue))
      }
    }

  }

  /**
    * Interface for RDD
    *
    * @param edges the edges of an input graph
    * @return RDD of connected components
    */
  def run(edges: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    val arr = edges.collect().iterator
    edges.sparkContext.parallelize[(Long, Long)](run(arr).toStream)
  }

}
