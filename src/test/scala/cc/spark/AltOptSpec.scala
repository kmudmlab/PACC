package cc.spark

import java.util.StringTokenizer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hmpark on 17. 3. 17.
  */
class AltOptSpec extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("cc.utils.PairExternalSorter").setLevel(Level.ERROR)
  Logger.getLogger(AltOpt.getClass).setLevel(Level.ERROR)
  Logger.getLogger("cc.spark.utils.PairExternalSorter").setLevel(Level.ERROR)

  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  "AltOpt" should "output the same result with UnionFindJob" in {

    val paths = Seq(
      getClass.getResource("/graphs/small/vline"),
      getClass.getResource("/graphs/small/line"),
      getClass.getResource("/graphs/small/facebook_686"),
      getClass.getResource("/graphs/small/w"),
      getClass.getResource("/graphs/facebook"),
      getClass.getResource("/graphs/grqc")
    )

    val conf = new SparkConf().setAppName("AltOpt-test").setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24m")

    val sc = new SparkContext(conf)

    val numPartitionsSet = Seq(1,2,4,8,16)

    for(path <- paths; numPartitions <- numPartitionsSet){

      logger.info(f"test( data: $path )")

      val true_result = UnionFind.run(sc.textFile(path.toString).map{ line =>
        val st = new StringTokenizer(line)
        (st.nextToken().toLong, st.nextToken().toLong)
      }).collect().distinct.sorted

      val res = AltOpt.run(path.toString, numPartitions, sc).collect().distinct.sorted

      res should be (true_result)

    }

    sc.stop()

  }

}
