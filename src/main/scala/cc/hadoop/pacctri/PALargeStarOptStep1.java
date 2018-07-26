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


package cc.hadoop.pacctri;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;


public class PALargeStarOptStep1 extends Configured implements Tool{

	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
    public long numChanges;
    public long inputSize;
	public long outSize;
    public long ccSize;
    public long inSize;

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 * @param verbose if true, it prints log verbosely.
	 */
	public PALargeStarOptStep1(Path input, Path output, boolean verbose){
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

		ToolRunner.run(new PALargeStarOptStep1(input, output, true), args);
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
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);


		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		FileSystem fs = FileSystem.get(getConf());

		fs.delete(output, true);


		if(fs.exists(input)){
			job.waitForCompletion(verbose);
			this.numChanges = job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
			this.inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			this.outSize = job.getCounters().findCounter(Counters.OUT_SIZE).getValue();
			this.ccSize = job.getCounters().findCounter(Counters.CC_SIZE).getValue();
			this.inSize = job.getCounters().findCounter(Counters.IN_SIZE).getValue();
		}

		return 0;
	}

    public static long code(long n, int p){
        return (n << 10) | p;
    }
    public static int part(long n, int p){
        return Long.hashCode(n) % p;
    }

    public static long decode_id(long n_raw){
        return n_raw >>> 10;
    }

    public static int decode_part(long n_raw){
        return (int) (n_raw & 0x3FF);
    }

	static public class ColorLargeStarMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

		int numPartitions;

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

            if (u.get() >= 0){
                ou.set(code(u.get(), part(v.get(), numPartitions)));
                ov.set(code(v.get(), part(u.get(), numPartitions)));
                context.write(ou, v);
                context.write(ov, u);
            }
            else{
                ov.set(code(v.get(), part(~u.get(), numPartitions)));
                context.write(ov, u);
            }
        }
    }

    static public class ColorLargeStarCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

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

            long u = decode_id(_u.get());

            long mu = Long.MAX_VALUE;

            for(LongWritable _v : values){

                long v = _v.get();

                if(v >= 0 && v < u){
                    mu = Math.min(mu, v);
                }
                else{
                    context.write(_u, _v);
                }
            }

            if(mu != Long.MAX_VALUE){
                ov.set(mu);
                context.write(_u, ov);
            }

        }
    }

	static public class ColorLargeStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		MultipleOutputs<LongWritable, LongWritable> mout;
		ExternalSorter sorter;

		int numPartitions;

		/**
		 * setup before execution
		 * @param context hadoop context
		 */
		@Override
		protected void setup(Context context){
            numPartitions = context.getConfiguration().getInt("numPartitions", 0);

			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

			mout = new MultipleOutputs<>(context);
		}

		LongWritable om = new LongWritable();
		LongWritable ov = new LongWritable();


        class PredicateWithMin implements Predicate<Long> {

            long mu;
            long u;
            boolean can_u_be_star = true;

            PredicateWithMin(long u, boolean is_upart) {
                this.u = u;
                this.mu = is_upart ? u : Long.MAX_VALUE;
            }

            public boolean test(Long _v) {

                boolean is_v_leaf = _v < 0;
                long v = is_v_leaf ? ~_v : _v;

                can_u_be_star = can_u_be_star && is_v_leaf;
                mu = Math.min(v, mu);

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

		    long _u = key.get();

			long u = decode_id(_u);
			long vp = decode_part(_u);
			int up = part(u, numPartitions);

			long numChanges = 0;

			long outSize = 0;
			long inSize = 0;
			long interSize = 0;

            PredicateWithMin lfilter = new PredicateWithMin(u, vp == up);

            Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
                    .map(LongWritable::get).filter(lfilter).iterator();

            Iterator<Long> uN_large = sorter.sort(it);

            long mu = lfilter.mu;
            boolean can_u_be_star = lfilter.can_u_be_star;

            om.set(mu);

            if(u == mu){

                boolean is_non_leaf_emitted = false;

                while(uN_large.hasNext()){
                    long v = uN_large.next();

                    if (v < 0) {
                        ov.set(~v);
                        mout.write(ov, om, "final/part-step1");
                        inSize++;
                    }
                    if (!is_non_leaf_emitted) {
                        ov.set(v);
                        mout.write(om, ov, "inter/part-step1");
                        is_non_leaf_emitted = true;
                        interSize++;
                    }
                    else {
                        ov.set(v);
                        mout.write(ov, om, "out/part-step1");
                        outSize++;
                    }

                }

            }
            else{


                while(uN_large.hasNext()){
                    long v = uN_large.next();

                    if(mu != v){

                        om.set(mu);

                        if(v < 0){
                            ov.set(~v);
                            mout.write(ov, om, "final/part-step1");
                            inSize++;
                        }
                        else {
                            ov.set(v);
                            mout.write(ov, om, "out/part-step1");
                            outSize++;
                        }

                        numChanges++;
                    }
                    else{ // v is local minimum (mu) and 0 <= v < u

                        v = v < 0 ? ~v : v;
                        v = can_u_be_star ? ~v : v;

                        ov.set(v);
                        om.set(u);
                        mout.write(om, ov, "inter/part-step1");
                        interSize++;
                    }

                }

                //if mu < u, we send mu to u because mu can be the minimum node among the neighbors of u
                if(mu < u){
                    ov.set(mu);
                    om.set(u);
                    mout.write(om, ov, "inter/part-step1");
                    interSize++;
                }

            }


			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
			context.getCounter(Counters.OUT_SIZE).increment(outSize);
            context.getCounter(Counters.IN_SIZE).increment(inSize);
            context.getCounter(Counters.INTER_SIZE).increment(interSize);


		}

        @Override
        protected void cleanup(
                Context context)
                throws IOException, InterruptedException {
            mout.close();
        }

	}


}
