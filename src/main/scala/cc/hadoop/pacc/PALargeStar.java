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


package cc.hadoop.pacc;

import cc.hadoop.Counters;
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
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;


public class PALargeStar extends Configured implements Tool{

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
	public PALargeStar(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), output.getName());
	}

	/**
	 * the main entry point
	 * @param args [0]: input file path, [1]: output file path, and tool runner arguments inherited from pacc
	 * @throws Exception of hadoop
	 */
	public static void main(String[] args) throws Exception{

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		ToolRunner.run(new PALargeStar(input, output, true), args);
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

		job.setMapperClass(ColorLargeStarMapper.class);
		job.setCombinerClass(ColorLargeStarCombiner.class);
		job.setReducerClass(ColorLargeStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);


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

	static public class ColorLargeStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

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

    static public class ColorLargeStarCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        private int numPartitions;
        long[] mcu;
        LongWritable ov = new LongWritable();

		/**
		 * setup before execution
		 * @param context of hadoop
		 */
		@Override
        protected void setup(Context context) {
            numPartitions = context.getConfiguration().getInt("numPartitions", 1);
            mcu = new long[numPartitions];
        }

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
            Arrays.fill(mcu, Long.MAX_VALUE);

            for(LongWritable _v : values){

                long v = _v.get();

                if(v < u){
                    int vp = (int) (v % numPartitions);
                    mcu[vp] = Math.min(mcu[vp], v);
                }
                else{
                    context.write(_u, _v);
                }
            }

            for(long v : mcu){
                if(v != Long.MAX_VALUE && v != u){
                    ov.set(v);
                    context.write(_u, ov);
                }
            }

        }
    }

	static public class ColorLargeStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		private int numPartitions;
		long[] mcu;
		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context hadoop context
		 */
		@Override
		protected void setup(Context context){

			numPartitions = context.getConfiguration().getInt("numPartitions", 1);
			mcu = new long[numPartitions];

			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

		}

		LongWritable om = new LongWritable();
		LongWritable ov = new LongWritable();

        class PredicateWithMin implements Predicate<Long> {

            long u;

            PredicateWithMin(long u) {
                this.u = u;
            }

            public boolean test(Long v) {

                int vp = v.hashCode() % numPartitions;

                mcu[vp] = Math.min(v, mcu[vp]);

                return u < v;
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

			long u = key.get();
			int uPartition = (int) (u % numPartitions);
			long numChanges = 0;

			Arrays.fill(mcu, Long.MAX_VALUE);

			mcu[uPartition] = u;

            PredicateWithMin lfilter = new PredicateWithMin(u);

			Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
					.map(LongWritable::get).filter(lfilter).iterator();


			Iterator<Long> uN_large = sorter.sort(it);

            long mu = Arrays.stream(mcu).min().getAsLong();

            while(uN_large.hasNext()){
                long v = uN_large.next();

                int vp = Long.hashCode(v) % numPartitions;
                long mcu_vp = mcu[vp];

                if(v != mcu_vp){
                    if(mcu_vp != u) numChanges++;

                    ov.set(v);
                    om.set(mcu_vp);
                    context.write(ov, om);
                }
                else{
                    if(mu != u) numChanges++;

                    ov.set(v);
                    om.set(mu);
                    context.write(ov, om);
                }
            }

			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);

		}

	}


}
