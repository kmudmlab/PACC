/*
 * PegasusN: Peta-Scale Graph Mining System (Pegasus v3.0)
 * Authors: Chiwan Park, Ha-Myung Park, U Kang
 *
 * Copyright (c) 2018, Ha-Myung Park, Chiwan Park, and U Kang
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
 * -------------------------------------------------------------------------
 * File: LargeStar.java
 * - the optimized largestar operation of pacc.
 * Version: 3.0
 */


package cc.hadoop.alt;

import cc.hadoop.utils.Counters;
import cc.hadoop.pacc.opt.TabularHashPartitioner;
import cc.hadoop.utils.ExternalSorter;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;


public class LargeStar extends Configured implements Tool{

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
	public LargeStar(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), output.getName());
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters inherited from pacc
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

		job.setMapperClass(LargeStarMapper.class);
		job.setCombinerClass(LargeStarCombiner.class);
		job.setReducerClass(LargeStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setPartitionerClass(TabularHashPartitioner.class);

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

	static public class LargeStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

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
            context.write(u, v);
            context.write(v, u);
        }
    }

    static public class LargeStarCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

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

            long u = _u.get();
            long mu = u;

            for(LongWritable _v : values){

                long v = _v.get();

                if(v < u) mu = Math.min(v, mu);
                else context.write(_u, _v);

            }

            if(mu != u){
            	ov.set(mu);
            	context.write(_u, ov);
			}

        }
    }

	static public class LargeStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context hadoop context
		 */
		@Override
		protected void setup(Context context){

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
				return v > u;
			}
		};

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

			final long u = key.get();
			long numChanges = 0;

			PredicateWithMin lfilter = new PredicateWithMin(u);

			Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
					.map(LongWritable::get).filter(lfilter).iterator();

			Iterator<Long> uN_large = sorter.sort(it);

			final long mu = lfilter.mu;
			om.set(mu);

			if(u == mu){
				while(uN_large.hasNext()){
					long v = uN_large.next();
					ov.set(v);
					context.write(ov, om);
				}
			}
			else{
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


}
