package cc.spark

import java.util.StringTokenizer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hmpark on 17. 3. 17.
  */
class PACCUnionSpec extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("cc.spark.utils.PairExternalSorter").setLevel(Level.WARN)

  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  "PACCUnion" should "output the same result with UnionFindJob" in {

    val paths = Seq(
      getClass.getResource("/graphs/small/vline"),
      getClass.getResource("/graphs/small/line"),
      getClass.getResource("/graphs/small/facebook_686"),
      getClass.getResource("/graphs/small/w"),
      getClass.getResource("/graphs/facebook"),
      getClass.getResource("/graphs/grqc")
    )

    val conf = new SparkConf().setAppName("PACCOpt-test").setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24m")

    val sc = new SparkContext(conf)


    val numPartitionsSet = Seq(1,2,4,8,16)

    val localThresholdSet = Seq(0, 100, 10000)


    for(path <- paths; numPartitions <- numPartitionsSet; localThreshold <- localThresholdSet){

      logger.info(f"test( data: $path, numPartitions: $numPartitions, localThreshold: $localThreshold )")

      val true_result = UnionFind.run(sc.textFile(path.toString).map{ line =>
        val st = new StringTokenizer(line)
        (st.nextToken().toLong, st.nextToken().toLong)
      }).collect().distinct.sorted

      val res = PACCUnion.run(path.toString, numPartitions, localThreshold, sc).collect().distinct.sorted

      res should be (true_result)

    }

    sc.stop()

  }

}
