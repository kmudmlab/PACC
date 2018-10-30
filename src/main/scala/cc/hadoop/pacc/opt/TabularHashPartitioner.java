/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: TabularHashPartitioner.java
 * - HashPartitioner with TabularHash.
 */

package cc.hadoop.pacc.opt;

import cc.hadoop.utils.TabularHash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TabularHashPartitioner extends Partitioner<LongWritable, LongWritable> {

    TabularHash H = TabularHash.getInstance();

    @Override
    public int getPartition(LongWritable longWritable, LongWritable longWritable2, int numPartitions) {
        return H.hash(longWritable.get()) % numPartitions;
    }
}
