/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: PASmallStarOpt.java
 * - The optimized PASmallStar operation for PACCOpt.
 */


package cc.hadoop.pacc.opt;

import cc.hadoop.utils.Counters;
import cc.hadoop.utils.ExternalSorter;
import cc.hadoop.utils.TabularHash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PASmallStarOpt extends Configured implements Tool{
	
	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long numChanges;
	public long inputSize;
	public long outSize;
	public long inSize;

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 * @param verbose if true, it prints log verbosely.
	 */
	public PASmallStarOpt(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
	}

	/**
	 * the main entry point
	 * @param args [0]: input file path, [1]: output file path, and tool runner parameters inherited from pacc
	 * @throws Exception by hadoop
	 */
	public static void main(String[] args) throws Exception{
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		ToolRunner.run(new PASmallStarOpt(input, output, true), args);
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters inherited from pacc
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf(); 
		
		Job job = Job.getInstance(conf, title);
		
		job.setJarByClass(this.getClass());
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(PASmallStarMapper.class);
		job.setReducerClass(PASmallStarReducer.class);
		job.setCombinerClass(PASmallStarCombiner.class);

        job.setPartitionerClass(TabularHashPartitioner.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem fs = FileSystem.get(conf);
		
		
		fs.delete(output, true);
		
		if(fs.exists(input)){
			job.waitForCompletion(verbose);
			this.numChanges = job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
			this.inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			this.outSize = job.getCounters().findCounter(Counters.OUT_SIZE).getValue();
			this.inSize = job.getCounters().findCounter(Counters.IN_SIZE).getValue();
		}
		
		return 0;
	}

	static public class PASmallStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

		/**
		 * the map function of SmallStarOpt.
		 * @param u source node
		 * @param v destination node
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void map(LongWritable u, LongWritable v, Context context)
				throws IOException, InterruptedException{
				context.write(u, v);
				context.write(v, u);
		}
	}


	static public class PASmallStarCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		/**
		 * the combiner function of SmallStarOpt
		 * @param key source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context)
				throws IOException, InterruptedException{
			
			long u = key.get();
			
			long m = u;
			
			for(LongWritable _v : values){
				long v = _v.get();
				if(v < u){
					context.write(key, _v);
				}
				else if(v > m) m = v;
			}
			
			if(u != m){
				context.write(key, new LongWritable(m));
			}
			
		}

	}

	static public class PASmallStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		/**
		 * cleanup after execution. It closes the output stream.
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException{
			
			mout.close();
		}

		TabularHash H = TabularHash.getInstance();
		private int numPartitions;
		private long[] mcu;
		MultipleOutputs<LongWritable, LongWritable> mout;

		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException{

			numPartitions = context.getConfiguration().getInt("numPartitions", 1);
			mcu = new long[numPartitions];

			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

			mout = new MultipleOutputs<LongWritable, LongWritable>(context);
			
		}
		
		LongWritable ok = new LongWritable();
		LongWritable ov = new LongWritable();


		class SmallIterator implements Iterator<Long> {

			long[] mcu;
			long u;

			private Iterator<LongWritable> origin;
			boolean isLeaf = true;
			long hd = -1;
			boolean hdDefined = false;

			public SmallIterator(Iterable<LongWritable> origin, long u, long[] mcu){
				this.origin = origin.iterator();
				this.mcu = mcu;
				this.u = u;
			}

			@Override
			public boolean hasNext() {

				if(hdDefined){
					return true;
				}
				else{
					while(origin.hasNext()){
						long v = origin.next().get();

						if(v > u) isLeaf = false;

						int vp = H.hash(v) % numPartitions;
						if(v < mcu[vp]) mcu[vp] = v;
						if(v < u){
							hd = v;
							hdDefined = true;
							return true;
						}
					}
					return false;
				}

			}

			@Override
			public Long next() {
				if(hdDefined || hasNext()){
					hdDefined = false;
					return hd;
				}
				else{
					throw new NoSuchElementException();
				}
			}
		}

		/**
		 * the reudce function of SmallStarOpt
		 * @param key source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context)
				throws IOException, InterruptedException{

			long u = key.get();
			int up = H.hash(u) % numPartitions;

			long numChanges = 0;
			long outSize = 0;
			long inSize = 0;

			Arrays.fill(mcu, Long.MAX_VALUE);
			mcu[up] = u;

			SmallIterator it_before = new SmallIterator(values, u, mcu);


			Iterator<Long> it = sorter.sort(it_before);

			boolean isLeaf = it_before.isLeaf;

			//mu: global minimum value.
			long mu = Arrays.stream(mcu).min().getAsLong();

			if(u != mcu[up]){ // u is not local minimum
				ok.set(u);
				ov.set(mcu[up]);

				if (isLeaf) {
					inSize++;
					mout.write(ok, ov, "in/part");
				}
				else{
					outSize++;
					mout.write(ok, ov, "out/part");
				}
			}
			else if (u != mu) { // u is the minimum in local but not in global
				outSize++;
				ok.set(isLeaf ? ~u : u);
				ov.set(mu);
				mout.write(ok, ov, "out/part");
			}

			while(it.hasNext()){
				long v = it.next();
				int vp = H.hash(v) % numPartitions;

				if(v != mcu[vp]){
					ok.set(v);
					ov.set(mcu[vp]);
					mout.write(ok, ov, "out/part");
					numChanges++;
				}
				else if(v != mu){
					ok.set(v);
					ov.set(mu);
					mout.write(ok, ov, "out/part");
					numChanges++;
				}
			}

			outSize += numChanges;

			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
			context.getCounter(Counters.OUT_SIZE).increment(outSize);
			context.getCounter(Counters.IN_SIZE).increment(inSize);
		}
		
	}
	
}
