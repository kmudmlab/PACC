package cc.spark

import java.util.StringTokenizer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hmpark on 17. 3. 17.
  */
class AltSpec extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("cc.utils.PairExternalSorter").setLevel(Level.WARN)
  Logger.getLogger(Alt.getClass).setLevel(Level.WARN)

  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  "Alt" should "output the same result with UnionFindJob" in {

    val paths = Seq(
      getClass.getResource("/graphs/small/vline"),
      getClass.getResource("/graphs/small/line"),
      getClass.getResource("/graphs/small/facebook_686"),
      getClass.getResource("/graphs/small/w"),
      getClass.getResource("/graphs/facebook"),
      getClass.getResource("/graphs/grqc")
    )

    val conf = new SparkConf().setAppName("Alt-test").setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24m")

    val sc = new SparkContext(conf)



    for(path <- paths){

      logger.info(f"test( data: $path )")

      val true_result = UnionFind.run(sc.textFile(path.toString).map{ line =>
        val st = new StringTokenizer(line)
        (st.nextToken().toLong, st.nextToken().toLong)
      }).collect().distinct.sorted

      val res = Alt.run(path.toString, sc).collect().distinct.sorted

      res should be (true_result)

    }

    sc.stop()

  }

}
