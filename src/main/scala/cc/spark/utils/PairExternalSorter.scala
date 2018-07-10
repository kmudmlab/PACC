package cc.spark.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.Random

import cc.io.{DirectReader, DirectWriter}
import cc.io.{DirectReader, DirectWriter}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable

class PairExternalSorter(basePaths: Array[String]){

  private val logger = Logger.getLogger(getClass)

  val localPath = new LocalPaths(basePaths, "pair-ext-sort")

  val KEY_SHIFT = 20
  val MAX_KEY: Long = Long.MaxValue >> KEY_SHIFT
  val BUFFER_SIZE: Int = 1 << KEY_SHIFT
  val INDEX_MASK: Long = BUFFER_SIZE - 1

  val packedKeysBuffer = new Array[Long](BUFFER_SIZE) // use 64MB
  val valuesBuffer = new Array[Long](BUFFER_SIZE) // use 64MB
  var usedBufferSize = 0
  def packedKeyWithIndex(key: Long, idx: Int): Long = (key << KEY_SHIFT) | idx
  def unpackKeyAndIndex(packedKey: Long): (Long, Int) =
    (packedKey >> KEY_SHIFT, (packedKey & INDEX_MASK).toInt)

  /**
    * Sort a given iterator. It consumes the given iterator and return a new one.
    *
    * @param it iterator to be sorted
    * @return sorted long iterator
    */
  def sort(it: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {
    mergeAll(localSort(it))
  }

  /**
    * Given a series of iterators, it merges all of them into one single sorted iterator.
    *
    * @param queue a series of iterators
    * @return merged and sorted iterator
    */
  private def mergeAll(queue: mutable.Queue[Iterator[(Long, Long)]]): Iterator[(Long, Long)] ={

    logger.info("Merging " + queue.size + " iterators.")
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
  private def mergeTwo(in1: Iterator[(Long, Long)], in2: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {

    val outPath = localPath.getTempPath("mergetwo")
    val out = new DirectWriter(outPath)
    var u: (Long, Long) = (0, 0)
    var v: (Long, Long) = (0, 0)
    u = in1.next
    v = in2.next

    def write(pair: (Long, Long)){
      out.write(pair._1)
      out.write(pair._2)
    }

    var remain = true

    while(remain){
      if(u._1 < v._1){
        write(u)
        if(in1.hasNext) u = in1.next
        else{
          write(v)
          remain = false
        }
      }
      else{
        write(v)
        if(in2.hasNext) v = in2.next
        else{
          write(u)
          remain = false
        }
      }
    }

    while(in1.hasNext) write(in1.next())
    while(in2.hasNext) write(in2.next())

    out.close()

    getDirectReaderIterator(outPath)

  }

  /**
    * This operation reads values from a given iterator, writes to the buffer, and sorts it.
    * If the input exceeds the size of the buffer, this operation spill the sorted
    * buffer out and continues until it reads all long values.
    *
    * @param it an iterator
    * @return a series of sorted iterators which are parts of the input iterator
    */
  def localSort(it: Iterator[(Long, Long)]): mutable.Queue[Iterator[(Long, Long)]] ={

    val queue = new mutable.Queue[Iterator[(Long, Long)]]()

    while(it.hasNext){
      val usedBufferSize = acquireNextBuffer(it)
      util.Arrays.sort(packedKeysBuffer, 0, usedBufferSize)

      if(it.hasNext){
        val tPath = localPath.getTempPath("localsort")

        val out = new DirectWriter(tPath)

        for(i <- 0 until usedBufferSize){
          val (key, idx) = unpackKeyAndIndex(packedKeysBuffer(i))
          out.write(key)
          out.write(valuesBuffer(idx))
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
    * Fill the buffer with next values.
    *
    * @param it an iterator
    * @return the number of values read
    */
  def acquireNextBuffer(it: Iterator[(Long, Long)]): Int ={
    var i = 0

    while(it.hasNext && i < BUFFER_SIZE){
      val (k, v) = it.next()
      packedKeysBuffer(i) = packedKeyWithIndex(k, i)
      valuesBuffer(i) = v
      i += 1
    }

    i
  }

  /**
    * Iterator for the buffer.
    *
    * @param limit to read
    * @return an iterator from the buffer
    */
  def getBufferIterator(limit: Int): Iterator[(Long, Long)] = new Iterator[(Long, Long)] {
    assert(limit <= BUFFER_SIZE)
    private var i: Int = 0
    override def hasNext: Boolean = i < limit

    override def next(): (Long, Long) = {
      val (key, idx) = unpackKeyAndIndex(packedKeysBuffer(i))
      val next = (key, valuesBuffer(idx))
      i += 1
      next
    }
  }

  /**
    * Iterator for a sorted file.
    * !IMPORTANT this operation delete the input file after it reads all values.
    *
    * @param path of a sorted file
    * @return an iterator for the sorted file
    */
  def getDirectReaderIterator(path: String): Iterator[(Long, Long)] ={

    new Iterator[(Long, Long)] {
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

      override def next(): (Long, Long) = if(hasNext) (dr.readLong(), dr.readLong()) else Iterator.empty.next()
    }

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
