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
 * File: PACC.scala
 * - The spark version of PACC. It finds connected components in a graph.
 */

package cc.spark

import java.util.StringTokenizer

import cc.spark.utils.FilteringOps._
import cc.spark.utils.PartImplicitWrapper._
import cc.spark.utils.StarGroupOps._
import cc.spark.utils.{LongExternalSorter, SerializableConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** PACC for finding connected components.
  * Two ways to run this algorithm:
  * - Using spark-submit in CLI.
  * - Calling [[PACC.run()]] method.
  */
object PACC{

  private val logger = Logger.getLogger(getClass)

  case class Config(localThreshold: Int = 100000, numPartitions: Int = 80,
                    inputPath: String = "", outputPath: String = "")

  val APP_NAME: String = "pacc"
  val VERSION: String = "0.1"

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config](APP_NAME) {
      head(APP_NAME, VERSION)

      opt[Int]('t', "localThreshold")
        .action((x, c) => c.copy(localThreshold = x))
        .text("if the number of remaining edges are lower than this value, " +
          "pacc run a single machine algorithm (LocalCC). (default: 100000)")

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

        logger.info(f"inputPath: ${opts.inputPath}, output: ${opts.outputPath}, " +
          f"localTrheshold: ${opts.localThreshold}, numPartitions: ${opts.numPartitions}")

        val conf = new SparkConf().setAppName("[" + APP_NAME + "]" + opts.inputPath)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer", "24m")

        val sc = new SparkContext(conf)

        FileSystem.get(sc.hadoopConfiguration).delete(new Path(opts.outputPath), true)

        run(opts.inputPath, opts.numPartitions, opts.localThreshold, sc)
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
    * @param localThreshold if the number of remaining edges are lower than this value,
    *                      pacc run a single machine algorithm (LocalCC).
    * @param sc spark context.
    * @return an RDD containing connected components
    */
  def run(inputPath: String, numPartitions: Int,
          localThreshold: Int, sc: SparkContext): RDD[(Long, Long)] = {

    val tmpPath = inputPath + ".pacc.tmp"

    val t0 = System.currentTimeMillis()

    // sketch after initialize
    var out = sc.textFile(inputPath).map{ line =>
      val st = new StringTokenizer(line)
      val u = st.nextToken().toLong
      val v = st.nextToken().toLong
      (u, v)
    }.mapPartitions{ it =>
      UnionFind.run(it)
    }

    // localization
    out = localization(out, numPartitions)


    var numEdges = out.count()
    logger.info(s"# edges: $numEdges")

    val t1 = System.currentTimeMillis()

    var converge = false

    var round = 0

    // partitioning step
    do{
      if(numEdges > localThreshold) {

        val t00 = System.currentTimeMillis()

        val (lout, l_change, lout_size, lcc_size, lin_size) = largeStar(out, numPartitions, round, tmpPath)
        val t01 = System.currentTimeMillis()

        val (sout, s_change, sout_size, sin_size) = smallStar(lout, numPartitions, round, tmpPath)
        val t02 = System.currentTimeMillis()

        val ltime = (t01-t00)/1000.0
        val stime = (t02-t01)/1000.0
        val ttime = (t02-t00)/1000.0

        out = sout

        logger.info(f"round($round) - lout: $lout_size, lcc: $lcc_size, lin: $lin_size, sout: $sout_size, sin: $sin_size, " +
          f"lchange: $l_change, schange: $s_change")

        converge = l_change == 0 && s_change == 0
        numEdges = sout_size

        round += 1
      }
      else{
        //do LocalCC
        out = UnionFind.run(out.map{ case (u, v) => (if (u < 0) ~u else u, v) })
        converge = true
      }
    }while(!converge)


    val fs = FileSystem.get(sc.hadoopConfiguration)

    val t2 = System.currentTimeMillis()

    if(fs.exists(new Path(tmpPath))){
      val others = sc.sequenceFile[Long, Long](tmpPath)
      out = out ++ others
    }

    // computation step
    val res = ccComputation(out, numPartitions)

    val t3 = System.currentTimeMillis()

    fs.deleteOnExit(new Path(tmpPath))

    val itime = (t1-t0)/1000.0
    val rtime = (t2-t1)/1000.0
    val ctime = (t3-t2)/1000.0
    val ttime = (t3-t0)/1000.0
    val inputFileName = inputPath.split("/").last

    println(s"$APP_NAME\t$inputFileName\t$localThreshold\t$numPartitions\t$round\t$itime\t$rtime\t$ctime\t$ttime")

    res
  }

  /**
    * CC-Computation operation.
    * This operation conducts LocalCC in each partition.
    *
    * @param remains input RDD
    * @param numPartitions the number of partitions
    * @return final output RDD containing connected components
    */
  def ccComputation(remains: RDD[(Long, Long)], numPartitions: Int): RDD[(Long, Long)] = {
    val res = remains.map{ case (u, v) => (if(u < 0) ~u else u, v) }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitions(edges => UnionFind.run(edges)).persist()

    res.count()

    res
  }



  def localization(inputRDD: RDD[(Long, Long)], numPartitions: Int): RDD[(Long, Long)] ={



    val sc = inputRDD.sparkContext

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val mod = inputRDD.getNumPartitions

    inputRDD.map{ case (u, v) =>
      val u_part = u.part(numPartitions)
      val v_enc = v.encode(u_part)

      def part(_x: Long): Int = {
        val x = _x.hashCode()
        val rawMod = x % mod
        rawMod + (if (rawMod < 0) mod else 0)
      }

//      println(part(v_enc), (v_enc, (v, u_part), u))

      (v_enc, u)
    }.starGrouped()
      .mapPartitions{ it =>

        val longExternalSorter = new LongExternalSorter(tmpPaths)

        def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] ={
          val (_u, uN) = x
          val u = _u.nodeId

          var mu = Long.MaxValue

          val _uNStream = uN.map { v =>

            if(mu > v) mu = v

            v
          }

          val uNStream = longExternalSorter.sort(_uNStream)

          uNStream.map{ v =>
            if(v != mu){
              (v, mu)
            }
            else{
              (v, u)
            }
          }

        }

        it.flatMap{processNode}


      }.persist(StorageLevel.DISK_ONLY)
  }

  /**
    * PA-Large-Star Operation.
    * For each node n, this operation links each large neighbor v to the minimum node mcu(p(v))
    * in the same partition p(v) that contains the neighbor v.
    *
    * @param inputRDD the input rdd
    * @param numPartitions the number of partitions
    * @param round current round number
    * @param tmpPath the temporary path to save the intermediate results
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'cc' edges, # filtered 'in' edges)
    */
  def largeStar(inputRDD: RDD[(Long, Long)], numPartitions: Int,
                round: Int, tmpPath: String): (RDD[(Long, Long)], Long, Long, Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator
    val LCC_SIZE = sc.longAccumulator
    val LIN_SIZE = sc.longAccumulator
    val LOUT_SIZE = sc.longAccumulator

    val groupedRDD = inputRDD.flatMap{
      case (u, v) =>
        if (u < 0) Seq((v, u))
        else Seq((u, v), (v, u))
    }.starGrouped()

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val res_all = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Boolean, Long, Long)] ={
        val (u, uN) = x

        val mcu = Array.fill[Long](numPartitions)(Long.MaxValue)
        mcu(u.mod(numPartitions)) = u

        var isStar = true

        val _uN_large = uN.filter { v_raw =>
          val v = if (v_raw >= 0) {
            isStar = false
            v_raw
          } else ~v_raw

          val vp = v.mod(numPartitions)
          mcu(vp) = Math.min(v, mcu(vp))

          v > u
        }

        val uN_large = longExternalSorter.sort(_uN_large)

        val mu = mcu.min

        if (isStar) uN_large.map { v_raw => LCC_SIZE.add(1); (false, ~v_raw, u) }
        else{
          uN_large.map{ v_raw =>
            val vIsLeaf = v_raw < 0
            val v: Long = if(vIsLeaf) ~v_raw else v_raw

            val vp = v.mod(numPartitions)
            val mcu_vp = mcu(vp)

            if(v != mcu_vp) {

              if(mcu_vp != u) NUM_CHANGES.add(1)

              if(vIsLeaf){
                LIN_SIZE.add(1)
                (false, v, mcu_vp)
              }
              else{
                LOUT_SIZE.add(1)
                (true, v, mcu_vp)
              }
            }
            else{// v is a local minimum but not the global minimum because 'it' has only large neighbors.
              LOUT_SIZE.add(1)

              if(mu != u) NUM_CHANGES.add(1)
              (true, v, mu)
            }
          }
        }
      }

      it.flatMap{processNode}


    }

    val hconf = new SerializableConfiguration(sc.hadoopConfiguration)
    val lout = res_all.filtered(tmpPath, f"large-$round%05d", hconf).persist(StorageLevel.DISK_ONLY)
    lout.count()

    inputRDD.unpersist(false)

    (lout, NUM_CHANGES.value, LOUT_SIZE.value, LCC_SIZE.value, LIN_SIZE.value)

  }

  /**
    * PA-Small-Star Operation.
    * For each node n, this operation links each small neighbor v to the minimum node mcu(p(v))
    * in the same partition p(v) that contains the neighbor v.
    *
    * @param inputRDD the input rdd
    * @param numPartitions the number of partitions
    * @param round current round number
    * @param tmpPath the temporary path to save the intermediate results
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'in' edges)
    */
  def smallStar(inputRDD: RDD[(Long, Long)], numPartitions: Int,
                round: Int, tmpPath: String): (RDD[(Long, Long)], Long, Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator
    val SIN_SIZE = sc.longAccumulator
    val SOUT_SIZE = sc.longAccumulator

    val groupedRDD = inputRDD.flatMap{
      case (u, v) => Seq((u, v), (v, u))
    }.starGrouped()

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val res_all = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Boolean, Long, Long)] ={
        val (u, uN) = x

        val mcu = Array.fill[Long](numPartitions)(Long.MaxValue)
        val up = u.mod(numPartitions)
        mcu(up) = u

        var isLeaf = true

        val _uN_small = uN.filter { v =>

          if(v > u) isLeaf = false
          val vp = v.mod(numPartitions)
          mcu(vp) = Math.min(v, mcu(vp))

          v < u
        }

        val uN_small = longExternalSorter.sort(_uN_small)

        val mu = mcu.min

        val sout = uN_small filter {_ != mu} map { v =>

          val vp = v.mod(numPartitions)

          NUM_CHANGES.add(1)
          SOUT_SIZE.add(1)

          if (v != mcu(vp))
            (true, v, mcu(vp))
          else // v is mcu but not mu
            (true, v, mu)

        }

        if (u != mcu(up)) {
          if (isLeaf) {
            SIN_SIZE.add(1)
            sout ++ Iterator((false, u, mcu(up)))
          }
          else {
            SOUT_SIZE.add(1)
            sout ++ Iterator((true, u, mcu(up)))
          }
        }
        else if (u != mu) {
          SOUT_SIZE.add(1)
          sout ++ Iterator((true, if (isLeaf) ~u else u, mu))
        }
        else sout

      }

      it.flatMap{processNode}

    }

    val hconf = new SerializableConfiguration(sc.hadoopConfiguration)

    val sout = res_all.filtered(tmpPath, f"small-$round%05d", hconf).persist(StorageLevel.DISK_ONLY)

    sout.count()

    inputRDD.unpersist(false)

    (sout, NUM_CHANGES.value, SOUT_SIZE.value, SIN_SIZE.value)

  }

}
