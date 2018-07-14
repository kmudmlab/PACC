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
 * File: Finalization.java
 * - cc-computation step of pacc.
 * Version: 3.0
 */

package cc.hadoop.altopt;

import cc.hadoop.utils.LongPairWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class Finalization extends Configured implements Tool{
	
	private final Path output;
    private final Path input;
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

		job.setMapperClass(FinalizationMapper.class);
		job.setReducerClass(FinalizationReducer.class);
		job.setCombinerClass(FinalizationCombiner.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongPairWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem.get(getConf()).delete(output, true);
		
		job.waitForCompletion(verbose);
		
		inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		
		return 0;
	}

	public static class FinalizationMapper extends Mapper<LongWritable, LongWritable, LongPairWritable, NullWritable>{

	    NullWritable nul = NullWritable.get();
	    LongPairWritable out = new LongPairWritable();

        @Override
        protected void map(LongWritable u, LongWritable v, Context context) throws IOException, InterruptedException {

            long uid = CopyUtil.nodeId(u.get());
            long vid = CopyUtil.nodeId(v.get());

            if(uid == vid) return;

            u.set(uid);
            v.set(vid);

            out.set(uid, vid);

            context.write(out, nul);

        }
    }

    public static class FinalizationReducer extends Reducer<LongPairWritable, NullWritable, LongWritable, LongWritable> {

	    LongWritable ou = new LongWritable();
	    LongWritable ov = new LongWritable();
        @Override
        protected void reduce(LongPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            ou.set(key.i);
            ov.set(key.j);

            context.write(ou, ov);
        }
    }

    public static class FinalizationCombiner extends Reducer<LongPairWritable, NullWritable, LongPairWritable, NullWritable>{

	    NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(LongPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, nul);

        }
    }

}
