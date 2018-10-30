/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Finalization.java
 * - CC-Computation step of PACC and PACCOpt.
 */

package cc.hadoop.pacc.opt;

import cc.hadoop.UnionFind;
import cc.hadoop.utils.LongPairWritable;
import cc.hadoop.utils.TabularHash;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

public class Finalization extends Configured implements Tool{
	
	private final Path output;
	private final String title;
	private final boolean verbose;
	private final int numRounds;
	public long inputSize;

	/**
	 * constructor
	 * @param output file path
	 * @param numRounds the number of rounds conducted until now.
	 * @param verbose if true, log is printed verbosely.
	 */
	public Finalization(Path output, int numRounds, boolean verbose){
		this.output = output;
		this.verbose = verbose;
		this.numRounds = numRounds;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), output.getName());
	}

	/**
	 * submit the hadoop job
	 * @param args not used
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Job job = new Job(getConf(), title);
		
		job.setJarByClass(this.getClass());
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(LargeStarMapper.class);
		job.setReducerClass(LargeStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setPartitionerClass(TabularHashPartitioner.class);

		FileSystem fs = FileSystem.get(getConf());
		
		
		for(int i=1; i<=numRounds; i++){
			if(fs.exists(output.suffix("_" + i + "/in"))){
				FileInputFormat.addInputPath(job, output.suffix("_" + i + "/in"));
			}
			if(fs.exists(output.suffix("_large_" + i + "/final"))){
				FileInputFormat.addInputPath(job, output.suffix("_large_" + i + "/final"));
			}
			
		}
		
		if(fs.exists(output.suffix("_" + numRounds + "/out"))){
			FileInputFormat.addInputPath(job, output.suffix("_" + numRounds + "/out"));
		}
		
		
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem.get(getConf()).delete(output, true);
		
		job.waitForCompletion(verbose);
		
		inputSize = job.getCounters().findCounter(Counter.MAP_INPUT_RECORDS).getValue();
		
		return 0;
	}

	static public class LargeStarMapper extends Mapper<LongWritable, LongWritable, IntWritable, LongPairWritable>{

		TabularHash H = TabularHash.getInstance();
		int numPartitions;

		/**
		 * setup before execution
		 * @param context hadoop context
		 * @throws IOException exception by hadoop
		 * @throws InterruptedException exception by hadoop
		 */
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException{
			
			numPartitions = context.getConfiguration().getInt("numPartitions", 0);
		}

		IntWritable p = new IntWritable();
		LongPairWritable edge = new LongPairWritable();

		/**
		 * the map function.
		 * @param u source node
		 * @param v destination node
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void map(LongWritable u, LongWritable v,
				Context context)
				throws IOException, InterruptedException{
			long uu = u.get();
			long vv = v.get();
			
			uu = uu < 0 ? -uu - 1: uu;
			vv = vv < 0 ? -vv - 1: vv;
			
			p.set((H.hash(uu) % numPartitions));
			edge.set(uu, vv);
			context.write(p, edge);
		}
		
	}
	
	static public class LargeStarReducer extends Reducer<IntWritable, LongPairWritable, LongWritable, LongWritable>{

		Context context;
		LongWritable ou = new LongWritable();
		LongWritable ov = new LongWritable();
		
		/**
		 * the reduce function.
		 * It find connected components in a partition.
		 * @param key the partition number. not used.
		 * @param values edges
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void reduce(IntWritable key, Iterable<LongPairWritable> values,
				Context context)
				throws IOException, InterruptedException{

			UnionFind uf = new UnionFind();

			Iterator<LongPairWritable> it = uf.run(values.iterator());

			while(it.hasNext()){
				LongPairWritable pair = it.next();
				ou.set(pair.i);
				ov.set(pair.j);
				context.write(ou, ov);
			}

		}

	}
	
}
