/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: SerializableConfiguration.scala
 * - serializable wrapper for hadoop configuration
 */

package cc.spark.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

/**
  * Serializable wrapper for configuration
  * @param value
  */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new JobConf(false)
    value.readFields(in)
  }
}
