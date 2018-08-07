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


package cc.hadoop.paccopt;

import cc.hadoop.Counters;
import cc.hadoop.utils.ExternalSorter;
import cc.hadoop.utils.TabularHash;
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


public class Localization extends Configured implements Tool{

	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 * @param verbose if true, it prints log verbosely.
	 */
	public Localization(Path input, Path output, boolean verbose){
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

		ToolRunner.run(new Localization(input, output, true), args);
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

		job.setMapperClass(LocalizationMapper.class);
		job.setReducerClass(LocalizationReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);


		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		FileSystem fs = FileSystem.get(getConf());

		fs.delete(output, true);


		if(fs.exists(input)){
			job.waitForCompletion(verbose);
		}

		return 0;
	}



	public static long code(long n, int p){
	    return (n << 10) | p;
    }

    public static long decode_id(long n_raw){
	    return n_raw >>> 10;
    }

    public static int decode_part(long n_raw){
	    return (int) (n_raw & 0x3FF);
    }

	static public class LocalizationMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

		TabularHash H = TabularHash.getInstance();
		int numPartitions;

	    LongWritable ou = new LongWritable();

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
		    int upart = H.hash(u.get()) % numPartitions;
		    ou.set(code(v.get(), upart));
            context.write(ou, u);
        }
    }

	static public class LocalizationReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

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
            long mpu;
            long u;

            PredicateWithMin(long u) {
                this.u = u;
                this.mpu = Long.MAX_VALUE;
            }

            public boolean test(Long v) {
                if(mpu > v) mpu = v;
                return true;
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

			long u_raw = key.get();
			long u = decode_id(u_raw);

			PredicateWithMin lfilter = new PredicateWithMin(u);

            Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
                    .map(LongWritable::get).filter(lfilter).iterator();

			Iterator<Long> uN_iterator = sorter.sort(it);

			long mpu = lfilter.mpu;

			while(uN_iterator.hasNext()){
			    long v = uN_iterator.next();

			    if(v != mpu){
			        ov.set(v);
			        om.set(mpu);
                    context.write(ov, om);
                }
                else{
			        ov.set(v);
			        om.set(u);
			        context.write(ov, om);
                }
            }

		}

	}


}
