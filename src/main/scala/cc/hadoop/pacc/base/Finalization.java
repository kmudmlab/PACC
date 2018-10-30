/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Finalization.java
 * - CC-Computation step of PACCBase.
 */


package cc.hadoop.pacc.base;

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

    private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long inputSize;

	/**
	 * constructor
	 * @param output file path
	 * @param verbose if true, log is printed verbosely.
	 */
	public Finalization(Path input, Path output, boolean verbose){
	    this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), output.getName());
	}

	/**
	 * submit the hadoop job
	 * @param args not used
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Job job = Job.getInstance(getConf(), title);
		
		job.setJarByClass(this.getClass());
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(LargeStarMapper.class);
		job.setReducerClass(LargeStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem.get(getConf()).delete(output, true);
		
		job.waitForCompletion(verbose);
		
		inputSize = job.getCounters().findCounter(Counter.MAP_INPUT_RECORDS).getValue();
		
		return 0;
	}

	static public class LargeStarMapper extends Mapper<LongWritable, LongWritable, IntWritable, LongPairWritable>{

		int numPartitions;
		TabularHash H = TabularHash.getInstance();

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
		 * @param _u source node
		 * @param _v destination node
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void map(LongWritable _u, LongWritable _v, Context context) throws IOException, InterruptedException{
			long u = _u.get();
			long v = _v.get();

			p.set(H.hash(u) % numPartitions);
			edge.set(u, v);
			context.write(p, edge);
		}
		
	}
	
	static public class LargeStarReducer extends Reducer<IntWritable, LongPairWritable, LongWritable, LongWritable>{

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
