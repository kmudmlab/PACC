/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: SmallStar.java
 * - SmallStar operation of the alternating algorithm.
 */


package cc.hadoop.alt;

import cc.hadoop.utils.Counters;
import cc.hadoop.pacc.opt.TabularHashPartitioner;
import cc.hadoop.utils.ExternalSorter;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class SmallStar extends Configured implements Tool{
	
	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long numChanges;
	public long inputSize;
	public long outSize;

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 * @param verbose if true, it prints log verbosely.
	 */
	public SmallStar(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters
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
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(SmallStarReducer.class);

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
			this.outSize = job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
		}
		
		return 0;
	}

	static public class SmallStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		MultipleOutputs<LongWritable, LongWritable> mout;

		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException{

			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

		}
		
		LongWritable om = new LongWritable();
		LongWritable ov = new LongWritable();

        class PredicateWithMin implements Predicate<Long> {
            long mu;
            long u;

            PredicateWithMin(long u) {
                this.u = u;
                this.mu = u;
            }

            public boolean test(Long v) {
                if (v < mu) mu = v;
                return true;
            }
        };

		/**
		 * the reudce function of SmallStar
		 * @param key source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context)
                throws IOException, InterruptedException {

			long u = key.get();

			long numChanges = 0;

            PredicateWithMin sfilter = new PredicateWithMin(u);

            Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
                    .map(LongWritable::get).filter(sfilter).iterator();

			Iterator<Long> uN_small  = sorter.sort(it);

            final long mu = sfilter.mu;
            om.set(mu);


			ov.set(u);
            context.write(ov, om);

            while(uN_small.hasNext()){
                long v = uN_small.next();

                if(v != mu) {
                    ov.set(v);
                    context.write(ov, om);
                    numChanges++;
                }
            }

			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
		}
		
	}
	
}
