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
