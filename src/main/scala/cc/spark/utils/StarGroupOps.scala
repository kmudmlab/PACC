package cc.spark.utils

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

/**
  * Created by hmpark on 17. 7. 27.
  */
object StarGroupOps {
  implicit class StarRDDOp(rdd: RDD[(Long, Long)]){
    def starGrouped(partitioner: Partitioner = new HashPartitioner(rdd.getNumPartitions)): RDD[(Long, Iterator[Long])] = {

      val hdconf = rdd.sparkContext.hadoopConfiguration
      val tmpPaths = hdconf.getTrimmedStrings("yarn.nodemanager.local-dirs")

      rdd.partitionBy(new HashPartitioner(rdd.getNumPartitions))
        .mapPartitions { it =>
          new PairExternalSorter(tmpPaths).sort(it).starGrouped()
        }

    }
  }

  implicit class StarIteratorOp(it: Iterator[(Long, Long)]){

    def starGrouped()= new Iterator[(Long, Iterator[Long])] {
      var first: Option[(Long, Long)] = None
      var prev: GroupedIterator = _

      override def hasNext: Boolean = first.isDefined ||
        (prev != null && {
          first = prev.consumeAndGetHead
          first.isDefined
        }) ||
        (it.hasNext && {
          first = Some(it.next())
          first.isDefined
        })

      override def next(): (Long, Iterator[Long]) = {

        if(hasNext) {
          prev = new GroupedIterator(first, it)
          val res = (first.get._1, prev.map(_._2))
          first = None
          res
        }
        else Iterator.empty.next()

      }

      class GroupedIterator(first: Option[(Long, Long)], base: Iterator[(Long, Long)]) extends Iterator[(Long, Long)]{

        private var (hd, hdDefined): ((Long, Long), Boolean) = first match {
          case Some(x) => (x, true)
          case None => (null, false)
        }
        var tailConsumed: Boolean = false

        private var tail: Iterator[(Long, Long)] = base

        def hasNext = hdDefined || tail.hasNext && {

          val cur = tail.next()
          tailConsumed = true

          if(cur._1 == hd._1) hdDefined = true
          else tail = Iterator.empty

          hd = cur
          hdDefined

        }
        def next() = if (hasNext) { hdDefined = false; tailConsumed = false; hd } else Iterator.empty.next()

        def consumeAndGetHead: Option[(Long, Long)] = {
          while(hasNext) next

          if(tailConsumed) Some(hd)
          else None

        }
      }
    }
  }
}
