package cc.hadoop.paccopt;

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
