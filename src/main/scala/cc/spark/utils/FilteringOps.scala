/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: FilteringOpts.scala
 */

package cc.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, SequenceFile}
import org.apache.spark.rdd.RDD

/** Implicit conversions and helpers for [[cc.spark.PACC]], [[cc.spark.PACCOpt]], and [[cc.spark.PACCBase]]. */
object FilteringOps {

  implicit class FilteredRDD(rdd: RDD[(Boolean, Long, Long)]) {

    /**
      * It returns a new RDD consisting of only items whose first element is true.
      * Items whose first element is false are written to the file of `path`.
      *
      * @param path the path of a directory where output files will be placed
      * @param prefix the prefix of the name of an output file for items filtered out
      * @param conf serializable hadoop configuration
      * @return rdd without false
      */
    def filtered(path: String, prefix: String, conf: SerializableConfiguration): RDD[(Long,Long)] ={

      rdd.mapPartitionsWithIndex{
        case (partitionId, it) =>
          val filePath = f"$path/$prefix-$partitionId%05d"
          it.filtered(filePath, conf.value)
      }
    }
  }

  implicit class FilteredIterator(it: Iterator[(Boolean, Long, Long)]) {

    /**
      * It returns a new iterator consisting of only items whose first element is true.
      * Items whose first element is false are written to the file of `path`.
      *
      * @param path the path of a directory where output files will be placed
      * @param prefix the prefix of the name of an output file for items filtered out
      * @param conf serializable hadoop configuration
      * @return rdd without false
      */
    def filtered(path: String, conf: Configuration)
    = new Iterator[(Long,Long)] {

      val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.keyClass(classOf[LongWritable]),
        SequenceFile.Writer.valueClass(classOf[LongWritable]),
        SequenceFile.Writer.file(new Path(path))
      )

      val aw = new LongWritable
      val bw = new LongWritable

      def close(){
        writer.close()
      }

      def pp(elem: (Boolean, Long, Long)): Boolean = {
        if(!elem._1){
          aw.set(elem._2)
          bw.set(elem._3)
          writer.append(aw, bw)
        }
        elem._1
      }

      private var hd: (Boolean, Long, Long) = _
      private var hdDefined: Boolean = false

      def hasNext: Boolean = hdDefined || {
        do {
          if (!it.hasNext){
            close()
            return false
          }
          hd = it.next()
        } while (!pp(hd))
        hdDefined = true
        true
      }

      def next(): (Long, Long) = if (hasNext) { hdDefined = false; (hd._2, hd._3) } else Iterator.empty.next()
    }
  }
}
