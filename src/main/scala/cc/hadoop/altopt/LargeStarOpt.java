/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: LargeStarOpt.java
 * - LargeStar operation of the optimized alternating algorithm.
 */


package cc.hadoop.altopt;

import cc.hadoop.utils.Counters;
import cc.hadoop.utils.ExternalSorter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class LargeStarOpt extends Configured implements Tool{

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
	public LargeStarOpt(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), output.getName());
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{


		Job job = Job.getInstance(getConf(), title);
		job.setJarByClass(this.getClass());

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(LargeStarOptMapper.class);
		job.setCombinerClass(LargeStarOptCombiner.class);
		job.setReducerClass(LargeStarOptReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setPartitionerClass(LargeStarOptPartitioner.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		FileSystem fs = FileSystem.get(getConf());

		fs.delete(output, true);


		if(fs.exists(input)){
			job.waitForCompletion(verbose);
			this.numChanges = job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
			this.inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			this.outSize = job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
		}

		return 0;
	}

	static public class LargeStarOptMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

	    int numPartitions;
	    long numChanges = 0;

	    LongWritable ou = new LongWritable();
	    LongWritable ov = new LongWritable();

        @Override
        protected void setup(Context context) {
            numPartitions = context.getConfiguration().getInt("numPartitions", 0);
        }

        /**
		 * the map function of LargeStarOpt.
		 * @param u source node
		 * @param v destination node
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
        protected void map(LongWritable u, LongWritable v, Context context) throws IOException, InterruptedException {

            long u_raw = u.get();
		    long v_raw = v.get();



		    if(u_raw == v_raw) return;

		    long u_comp = CopyUtil.comp(u_raw);
            long v_comp = CopyUtil.comp(v_raw);

		    if(v_comp < u_comp){
                long tmp = u_raw;
                u_raw = v_raw;
                v_raw = tmp;
            }

            long ulow = CopyUtil.low(u_raw);
            long vlow = CopyUtil.low(v_raw);

            long uid = CopyUtil.nodeId(u_raw);
            long vid = CopyUtil.nodeId(v_raw);

            if(CopyUtil.isHigh(u_raw) && CopyUtil.nonCopy(u_raw) && (uid != vid)){

		        long ui = CopyUtil.copy(u_raw, v_raw, numPartitions);

		        numChanges++;

		        ou.set(ulow);
		        ov.set(ui);
		        context.write(ou, ov);

		        ou.set(vlow);
		        context.write(ov, ou);

            }
            else{

		        ou.set(ulow);
		        ov.set(vlow);
		        context.write(ou, ov);
                context.write(ov, ou);

            }

        }

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
		}
	}

    static public class LargeStarOptCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        LongWritable ov = new LongWritable();

		/**
		 * the combiner function of LargeStarOpt
		 * @param _u source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
        @Override
        protected void reduce(LongWritable _u, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long u_raw = _u.get();
            long mu = u_raw;
            long u_comp = CopyUtil.comp(u_raw);
            long mu_comp = u_comp;

            for(LongWritable _v : values){

                long v_raw = _v.get();
                long v_comp = CopyUtil.comp(v_raw);

                if(v_comp < u_comp){
                    if(mu_comp > v_comp){
                        mu_comp = v_comp;
                        mu = v_raw;
                    }
                }
                else{
                    context.write(_u, _v);
                }
            }

            if(mu != u_raw){
            	ov.set(mu);
            	context.write(_u, ov);
			}

        }
    }

	static public class LargeStarOptReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		private int numPartitions;
		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context hadoop context
		 */
		@Override
		protected void setup(Context context){

			numPartitions = context.getConfiguration().getInt("numPartitions", 1);

			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

		}

		LongWritable om = new LongWritable();
		LongWritable ov = new LongWritable();

		class PredicateWithMin implements Predicate<Long> {
			long mu;
			long u_comp;
            long mu_comp;
			long u_raw;
			long uNSize = 0;
			long uid;

			PredicateWithMin(long u_raw) {
				this.u_raw = u_raw;
				this.mu = u_raw;
                this.u_comp = CopyUtil.comp(u_raw);
				this.mu_comp = u_comp;
                this.uid = CopyUtil.nodeId(u_raw);
			}

			public boolean test(Long v_raw) {
			    long v_comp = CopyUtil.comp(v_raw);
                if(mu_comp > v_comp){
                    mu_comp = v_comp;
                    mu = v_raw;
                }

                uNSize++;

				return u_comp < v_comp;
			}
		}

		/**
		 * the reduce function of LargeStarOpt
		 * @param key source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException{

			final long u_raw = key.get();
			long numChanges = 0;

			PredicateWithMin lfilter = new PredicateWithMin(u_raw);

			Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
					.map(LongWritable::get).filter(lfilter).iterator();

			Iterator<Long> uN_large = sorter.sort(it);

			final long mu = lfilter.mu;
			final long muid = CopyUtil.nodeId(mu);
			final long uNSize = lfilter.uNSize;

			long u = (uNSize > numPartitions && CopyUtil.nonCopy(u_raw)) ? CopyUtil.high(u_raw) : u_raw;
            long uid = CopyUtil.nodeId(u);



			if(uid == muid){
                om.set(u);
				while(uN_large.hasNext()){
					long v = uN_large.next();

					ov.set(v);
					context.write(ov, om);
				}
			}
			else{
                om.set(mu);
				while(uN_large.hasNext()){
					long v = uN_large.next();

					ov.set(v);
					context.write(ov, om);
					numChanges++;

				}
			}

			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);

		}

	}


    static public class LargeStarOptPartitioner extends Partitioner<LongWritable, LongWritable> {
        @Override
        public int getPartition(LongWritable u, LongWritable v, int numPartitions) {

            int hash = CopyUtil.hash(u.get()) % numPartitions;
            if(hash < 0) hash = hash + numPartitions;

            return hash;
        }
    }
}
