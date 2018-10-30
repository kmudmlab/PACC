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
 * File: AltOpt.scala
 * - The spark version of the optimized alternating algorithm introduced in the following paper:
 *   Raimondas Kiveris, Silvio Lattanzi, Vahab Mirrokni, Vibhor Rastogi, and Sergei Vassilvitskii. 2014.
 *   Connected Components in MapReduce and Beyond. SOCC, 2014
 */

package cc.spark

import java.util.StringTokenizer

import cc.spark.utils.LongExternalSorter
import cc.spark.utils.StarGroupOps._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import utils.CopyImplicitWrapper._

/** Optimized Alternating algorithm for finding connected components.
  * Two ways to run this algorithm:
  * - Using spark-submit in CLI.
  * - Calling [[AltOpt.run()]] method.
  */
object AltOpt{

  private val logger = Logger.getLogger(getClass)

  case class Config(numPartitions: Int = 80, inputPath: String = "", outputPath: String = "")

  val APP_NAME: String = "alt-opt"
  val VERSION: String = "0.1"

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config](APP_NAME) {
      head(APP_NAME, VERSION)

      opt[Int]('p', "numPartition").required()
        .action((x, c) => c.copy(numPartitions = x))
        .text("the number of partitions. (default: 80)")

      arg[String]("input")
        .action((x, c) => c.copy(inputPath = x))
        .text("input file path.")

      arg[String]("output")
        .action((x, c) => c.copy(outputPath = x))
        .text("output file path.")
    }

    parser.parse(args, Config()) match {
      case Some(opts) =>

        logger.info(f"inputPath: ${opts.inputPath}, output: ${opts.outputPath}")

        val conf = new SparkConf().setAppName("[" + APP_NAME + "]" + opts.inputPath)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer", "24m")

        val sc = new SparkContext(conf)

        FileSystem.get(sc.hadoopConfiguration).delete(new Path(opts.outputPath), true)

        run(opts.inputPath, opts.numPartitions, sc)
          .map { case (u, v) => u + "\t" + v }
          .saveAsTextFile(opts.outputPath)

        sc.stop()

      case None =>
    }
  }

  /**
    * submit the spark job.
    *
    * @param inputPath input file path.
    * @param numPartitions the number of partitions
    * @param sc spark context.
    * @return an RDD containing connected components
    */
  def run(inputPath: String, numPartitions: Int, sc: SparkContext): RDD[(Long, Long)] = {

    val t0 = System.currentTimeMillis()

    //initialize
    var out = sc.textFile(inputPath).map{ line =>
      val st = new StringTokenizer(line)
      val u = st.nextToken().toLong
      val v = st.nextToken().toLong
      (u.toNode, v.toNode)
    }

    var numEdges = out.count()
    val t1 = System.currentTimeMillis()


    var converge = false

    var round = 0

    do{

      val t00 = System.currentTimeMillis()

      val (lout, l_change, lout_size) = largeStar(out, numPartitions)
      val t01 = System.currentTimeMillis()

      val (sout, s_change, sout_size) = smallStar(lout)
      val t02 = System.currentTimeMillis()

      val ltime = (t01-t00)/1000.0
      val stime = (t02-t01)/1000.0
      val ttime = (t02-t00)/1000.0

      out = sout

      logger.info(f"round($round) - lout: $lout_size, sout: $sout_size, " +
        f"lchange: $l_change, schange: $s_change")

//      println(s"star\t$round\t$lout_size\t$sout_size\t$l_change\t$s_change\t$ltime\t$stime\t$ttime")

      converge = l_change == 0 && s_change == 0
      numEdges = sout_size

      round += 1

    }while(!converge)

    val t2 = System.currentTimeMillis()

    val res = out.map{case (u, v) => (u.nodeId, v.nodeId)}.filter{case (u, v) => u != v}.persist(StorageLevel.DISK_ONLY)

    res.count()

    out.unpersist(false)

    val t3 = System.currentTimeMillis()

    val itime = (t1-t0)/1000.0
    val rtime = (t2-t1)/1000.0
    val ctime = (t3-t2)/1000.0
    val ttime = (t3-t0)/1000.0
    val inputFileName = inputPath.split("/").last

    println(s"$APP_NAME\t$inputFileName\t$numPartitions\t$round\t$itime\t$rtime\t$ctime\t$ttime")

    res
  }

  /**
    * Large-Star-Opt Operation.
    *
    * @param inputRDD the input rdd
    * @param numPartitions the number of partitions
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'cc' edges, # filtered 'in' edges)
    */
  def largeStar(inputRDD: RDD[(Long, Long)], numPartitions: Int): (RDD[(Long, Long)], Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator

    val groupedRDD = inputRDD.map { case (u, v) =>
      if (u.comp < v.comp) (u, v)
      else (v, u)
    }.filter { case (u, v) => u != v }
      .flatMap { case (u, v) =>
        if (u.isHigh && u.nonCopy && (u.nodeId != v.nodeId)) {

          val ui = u.copy(v, numPartitions)

          NUM_CHANGES.add(1)
          Seq((u.low, ui), (ui, v.low))
        }
        else {
          Seq((u.low, v.low), (v.low, u.low))
        }
      }
      .starGrouped(new Partitioner() {

        val p = inputRDD.getNumPartitions

        override def numPartitions: Int = p

        override def getPartition(key: Any): Int = {
          val u = key.asInstanceOf[Long]
          val x = u.hash % p
          if(x < 0) x + p else x
        }
      })

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val lout = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] ={
        val (u, uN) = x

        var mu = u
        var mu_comp = u.comp
        var uNSize = 0l


        val _uN_large = uN.filter { v =>

          if(mu_comp > v.comp){
            mu_comp = v.comp
            mu = v
          }

          uNSize += 1

          u.comp < v.comp
        }

        val uN_large = longExternalSorter.sort(_uN_large)

        val ou = if(uNSize > numPartitions && u.nonCopy) u.high else u

        if(ou.nodeId == mu.nodeId){
          uN_large.map{v => (v, ou)}
        }
        else{
          uN_large.map{v =>
            NUM_CHANGES.add(1)
            (v, mu)
          }
        }
      }

      it.flatMap{processNode}


    }.persist(StorageLevel.DISK_ONLY)

    val lout_size = lout.count()

    inputRDD.unpersist(false)

    (lout, NUM_CHANGES.value, lout_size)

  }

  /**
    * PA-Small-Star Operation.
    * For each node n, this operation links each small neighbor v to the minimum node mcu(p(v))
    * in the same partition p(v) that contains the neighbor v.
    *
    * @param inputRDD the input rdd
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'in' edges)
    */
  def smallStar(inputRDD: RDD[(Long, Long)]): (RDD[(Long, Long)], Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator

    val groupedRDD = inputRDD.map{case (u, v) =>
      if (u.comp < v.comp) {
        (v, u)
      }else {
        (u, v)
      }
    }.starGrouped()

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val sout = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] = {
        val (u, uN) = x

        var mu = u
        var mu_comp = u.comp

        val _uN_small = uN.map { v =>

          if(mu_comp > v.comp){
            mu_comp = v.comp
            mu = v
          }

          v
        }

        val uN_small = longExternalSorter.sort(_uN_small)

        (uN_small ++ Iterator(u)).filter(_ != mu) map { v =>

          if(v != u){
            NUM_CHANGES.add(1)
          }
          (v, mu)
        }

      }

      it.flatMap{processNode}

    }.persist(StorageLevel.DISK_ONLY)

    val sout_size = sout.count()

    inputRDD.unpersist(false)

    (sout, NUM_CHANGES.value, sout_size)

  }

}
