/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Alt.scala
 * - The spark version of the alternating algorithm introduced in the following paper:
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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** Alternating algorithm for finding connected components.
  * Two ways to run this algorithm:
  * - Using spark-submit in CLI.
  * - Calling [[Alt.run()]] method.
  */
object Alt{


  private val logger = Logger.getLogger(getClass)

  case class Config(inputPath: String = "", outputPath: String = "")

  val APP_NAME: String = "alt"
  val VERSION: String = "0.1"

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config](APP_NAME) {
      head(APP_NAME, VERSION)

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

        run(opts.inputPath, sc)
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
    * @param sc spark context.
    * @return an RDD containing connected components
    */
  def run(inputPath: String, sc: SparkContext): RDD[(Long, Long)] = {

    val t0 = System.currentTimeMillis()

    //initialize
    var out = sc.textFile(inputPath).map{ line =>
      val st = new StringTokenizer(line)
      val u = st.nextToken().toLong
      val v = st.nextToken().toLong
      (u, v)
    }

    var numEdges = out.count()
    val t1 = System.currentTimeMillis()



    var converge = false

    var round = 0

    do{

      val t00 = System.currentTimeMillis()

      val (lout, l_change, lout_size) = largeStar(out, round)
      val t01 = System.currentTimeMillis()

      val (sout, s_change, sout_size) = smallStar(lout, round)
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

    val itime = (t1-t0)/1000.0
    val rtime = (t2-t1)/1000.0
    val ttime = (t2-t0)/1000.0
    val inputFileName = inputPath.split("/").last

    println(s"$APP_NAME\t$inputFileName\t$round\t$itime\t$rtime\t$ttime")

    out
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


  /**
    * PA-Large-Star Operation.
    * For each node n, this operation links each large neighbor v to the minimum node mcu(p(v))
    * in the same partition p(v) that contains the neighbor v.
    *
    * @param inputRDD the input rdd
    * @param round current round number
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'cc' edges, # filtered 'in' edges)
    */
  def largeStar(inputRDD: RDD[(Long, Long)], round: Int): (RDD[(Long, Long)], Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator

    val groupedRDD = inputRDD.flatMap{
      case (u, v) =>
        Seq((u, v), (v, u))
    }.starGrouped()

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val lout = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] ={
        val (u, uN) = x

        var mu = u

        val _uN_large = uN.filter { v =>

          mu = mu min v

          v > u
        }

        val uN_large = longExternalSorter.sort(_uN_large)

        if(u == mu){
          uN_large.map((_, mu))
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
    * @param round current round number
    * @return (RDD for next round input, # changed edges, # of 'out' edges,
    *         # filtered 'in' edges)
    */
  def smallStar(inputRDD: RDD[(Long, Long)], round: Int): (RDD[(Long, Long)], Long, Long) = {

    val sc = inputRDD.sparkContext

    val NUM_CHANGES = sc.longAccumulator

    val groupedRDD = inputRDD.starGrouped()

    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val sout = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] = {
        val (u, uN) = x

        var mu = Long.MaxValue

        val _uN_small = uN.map { v =>

          mu = mu min v

          v
        }

        val uN_small = longExternalSorter.sort(_uN_small)

        (uN_small.filter(_ != mu) ++ Iterator(u)) map { v =>
          if (v != u) NUM_CHANGES.add(1)
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
