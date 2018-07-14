package cc.spark

import java.util.StringTokenizer

import cc.spark.utils.LongExternalSorter
import cc.spark.utils.StarGroupOps._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.CopyImplicitWrapper._

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
      (u, v)
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

      println(s"star\t$round\t$lout_size\t$sout_size\t$l_change\t$s_change\t$ltime\t$stime\t$ttime")

      converge = l_change == 0 && s_change == 0
      numEdges = sout_size

      round += 1

    }while(!converge)

    val t2 = System.currentTimeMillis()

    val res = out.map{case (u, v) => (u.nodeId, v.nodeId)}.filter{case (u, v) => u != v}.persist(StorageLevel.MEMORY_AND_DISK)

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
      if (u.nodeId < v.nodeId || ((u.nodeId == v.nodeId) && v.isCopy && u.nonCopy)) (u, v)
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
      .starGrouped()
//      .groupByKey().mapValues(x=> x.iterator)


    val tmpPaths = sc.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")

    val lout = groupedRDD.mapPartitions{ it =>

      val longExternalSorter = new LongExternalSorter(tmpPaths)

      def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] ={
        val (_u, uN) = x

        var mu = _u
        val u_comp = (_u.nodeId << 1) + (if(_u.isCopy) 1 else 0)
        var mu_comp = u_comp
        var uNSize = 0l


        val _uN_large = uN.filter { v =>

          val v_comp = (v.nodeId << 1) + (if(v.isCopy) 1 else 0)
          if(mu_comp > v_comp){
            mu_comp = v_comp
            mu = v
          }

          uNSize += 1

          u_comp < v_comp
        }

        val uN_large = longExternalSorter.sort(_uN_large)

        val u = if(uNSize > numPartitions && _u.nonCopy) _u.high else _u

        if(u.nodeId == mu.nodeId){
          uN_large.map{v => (v, u)}
        }
        else{
          uN_large.map{v =>
            NUM_CHANGES.add(1)
            (v, mu)
          }
        }
      }

      it.flatMap{processNode}


    }.persist(StorageLevel.MEMORY_AND_DISK)

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
      if (u.nodeId < v.nodeId || (u.nodeId == v.nodeId && u.nonCopy && v.isCopy)) {
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
        var mu_comp = (u.nodeId << 1) + (if(u.isCopy) 1 else 0)

        val _uN_small = uN.map { v =>

          val v_comp = (v.nodeId << 1) + (if(v.isCopy) 1 else 0)
          if(mu_comp > v_comp){
            mu_comp = v_comp
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

    }.persist(StorageLevel.MEMORY_AND_DISK)

    val sout_size = sout.count()

    inputRDD.unpersist(false)

    sout.foreach{case (u, v) => println(u.toTuple + "\t" + v.toTuple)}

    (sout, NUM_CHANGES.value, sout_size)

  }

}
