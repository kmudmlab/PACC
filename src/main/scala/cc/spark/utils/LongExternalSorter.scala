/**
  * PegasusN: Peta-Scale Graph Mining System (Pegasus v3.0)
  * Authors: Chiwan Park, Ha-Myung Park, U Kang
  *
  * Copyright (c) 2018, Ha-Myung Park, Chiwan Park, and U Kang
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
  * -------------------------------------------------------------------------
  * File: LongExternalSorter.scala
  * - external sorter for long iterators.
  * Version: 3.0
  */

package cc.spark.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.Random

import cc.io.{DirectReader, DirectWriter}
import cc.io.{DirectReader, DirectWriter}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable

class LongExternalSorter(basePaths: Array[String]){

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  val localPath = new LocalPaths(basePaths, "long-ext-sort")

  val KEY_SHIFT = 23
  val BUFFER_SIZE: Int = 1 << KEY_SHIFT

  val buffer = new Array[Long](BUFFER_SIZE) // use 64MB
  var usedBufferSize = 0

  /**
    * Sort a given long iterator. It consumes the given iterator and return a new one.
    *
    * @param it long iterator to be sorted
    * @return sorted long iterator
    */
  def sort(it: Iterator[Long]): Iterator[Long] = {
    mergeAll(localSort(it))
  }

  /**
    * Given a series of long iterators, it merges all of them into one single sorted iterator.
    *
    * @param queue a series of long iterators
    * @return merged and sorted iterator
    */
  private def mergeAll(queue: mutable.Queue[Iterator[Long]]): Iterator[Long] ={

    if(queue.isEmpty){Iterator.empty}
    else {
      while (queue.size > 1) queue.enqueue(mergeTwo(queue.dequeue(), queue.dequeue()))
      queue.dequeue()
    }
  }

  /**
    * Given two sorted iterator, it returns a new merged and sorted iterator.
    *
    * @param in1 an iterator
    * @param in2 another iterator
    * @return merged and sorted iterator
    */
  private def mergeTwo(in1: Iterator[Long], in2: Iterator[Long]): Iterator[Long] = {

    val outPath = localPath.getTempPath("mergetwo")
    val out = new DirectWriter(outPath)
    var prev = Long.MinValue
    var u: Long = 0
    var v: Long = 0
    u = in1.next
    v = in2.next

    var remain = true

    def write(x: Long): Unit ={
      if(prev != x){
        out.write(x)
        prev = x
      }
    }

    while(remain){
      if(u < v){
        write(u)
        if(in1.hasNext) u = in1.next
        else{
          write(v)
          remain = false
        }
      }
      else if (u > v){
        write(v)
        if(in2.hasNext) v = in2.next
        else{
          write(u)
          remain = false
        }
      }
      else{
        if(in1.hasNext) u = in1.next
        else{
          write(v)
          remain = false
        }
      }
    }

    while(in1.hasNext){
      write(in1.next())
    }

    while(in2.hasNext){
      write(in2.next())
    }

    out.close()

    getDirectReaderIterator(outPath)
  }

  /**
    * This operation reads long values from a given iterator, writes to the buffer, and sorts it.
    * If the input exceeds the size of the buffer, this operation spill the sorted
    * buffer out and continues until it reads all long values.
    *
    * @param it a long iterator
    * @return a series of sorted iterators which are parts of the input iterator
    */
  def localSort(it: Iterator[Long]): mutable.Queue[Iterator[Long]] ={

    val queue = new mutable.Queue[Iterator[Long]]()

    while(it.hasNext){
      val usedBufferSize = acquireNextBuffer(it)
      util.Arrays.sort(buffer, 0, usedBufferSize)

      if(it.hasNext){
        val tPath = localPath.getTempPath("localsort")

        val out = new DirectWriter(tPath)

        var prev = Long.MinValue
        for(i <- 0 until usedBufferSize){
          val next = buffer(i)
          if(prev != next) {
            out.write(next)
            prev = next
          }
        }
        out.close()

        queue.enqueue(getDirectReaderIterator(tPath))
      }
      else{
        queue.enqueue(getBufferIterator(usedBufferSize))
      }
    }

    queue
  }

  /**
    * Fill the buffer with next long values.
    *
    * @param it long iterator
    * @return the number of values read
    */
  def acquireNextBuffer(it: Iterator[Long]): Int ={
    var i = 0

    while(it.hasNext && i < BUFFER_SIZE){
      val v = it.next()
      buffer(i) = v
      i += 1
    }

    i
  }

  /**
    * Iterator for the buffer.
    *
    * @param limit to read
    * @return long iterator from the buffer
    */
  def getBufferIterator(limit: Int): Iterator[Long] = new Iterator[Long] {
    assert(limit <= BUFFER_SIZE)
    private var i: Int = 0
    private var prev: Long = Long.MinValue
    override def hasNext: Boolean ={
      while(i < limit && prev == buffer(i)) i += 1
      i < limit
    }

    override def next(): Long = {
      if(hasNext){
        val next = buffer(i)
        prev = next
        i += 1
        next
      }
      else Iterator.empty.next()
    }
  }

  /**
    * Iterator for a sorted file.
    * !IMPORTANT this operation delete the input file after it reads all values.
    *
    * @param path of a sorted file
    * @return long iterator for the sorted file
    */
  def getDirectReaderIterator(path: String): Iterator[Long] = new Iterator[Long] {
    var dr: DirectReader = _
    var state: Short = 0 //0: not initialized, 1:dr is open, 2: dr is closed.
    override def hasNext: Boolean = state match {
      case 0 =>
        dr = new DirectReader(path)
        state = 1
        hasNext
      case 1 =>
        val nxt = dr.hasNext
        if (!nxt) {
          dr.close()
          Files.delete(Paths.get(path))
          state = 2
        }
        nxt
      case 2 => false
    }

    override def next(): Long = if (hasNext) dr.readLong() else Iterator.empty.next()
  }

  class LocalPaths(basePaths: Array[String], name: String) {
    private val rand: Random = new Random

    for (tmpDir <- basePaths) {
      new File(tmpDir + "/" + name).mkdirs
    }

    private def getNextBasePath: String = basePaths(rand.nextInt(basePaths.length))

    def getTempPath(prefix: String): String = {
      var res: String = null
      do {
        val t: Int = rand.nextInt(Integer.MAX_VALUE)
        res = getNextBasePath + "/" + prefix + "-" + t
      } while (Files.exists(Paths.get(res)))
      res
    }
  }

}
