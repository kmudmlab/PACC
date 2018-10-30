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
 * File: SmallStarOpt.java
 * - SmallStar operation of the optimized alternating algorithm.
 */


package cc.hadoop.altopt;

import cc.hadoop.utils.Counters;
import cc.hadoop.utils.ExternalSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
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

public class SmallStarOpt extends Configured implements Tool{
	
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
	public SmallStarOpt(Path input, Path output, boolean verbose){
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
		
		job.setMapperClass(SmallStarMapper.class);
		job.setReducerClass(SmallStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setPartitionerClass(SmallStarOptPartitioner.class);

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

	static public class SmallStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

        @Override
        protected void map(LongWritable u, LongWritable v, Context context) throws IOException, InterruptedException {

            long u_raw = u.get();
            long v_raw = v.get();

            long uid = CopyUtil.nodeId(u_raw);
            long vid = CopyUtil.nodeId(v_raw);

            if(uid < vid || (uid == vid && CopyUtil.nonCopy(u_raw) && CopyUtil.isCopy(v_raw))){
                context.write(v, u);
            }
            else{
                context.write(u, v);
            }
        }
    }

	static public class SmallStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		MultipleOutputs<LongWritable, LongWritable> mout;

		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context of hadoop
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
            long mu_comp;
            long u;
            long u_comp;

            PredicateWithMin(long u) {
                this.u = u;
                this.mu = u;
                this.u_comp = CopyUtil.comp(u);
                this.mu_comp = u_comp;
            }

            public boolean test(Long v) {
                long v_comp = CopyUtil.comp(v);

                if(v_comp < mu_comp){
                    mu = v;
                    mu_comp = v_comp;
                }

                return true;
            }
        };

		/**
		 * the reudce function of SmallStarOpt
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

	static public class SmallStarOptPartitioner extends Partitioner<LongWritable, LongWritable> {
		@Override
		public int getPartition(LongWritable u, LongWritable v, int numPartitions) {

			int hash = CopyUtil.hash(u.get()) % numPartitions;
			if(hash < 0) hash = hash + numPartitions;

			return hash;
		}
	}
	
}
