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
 * File: Initialization.java
 * - initialization step of pacc.
 * Version: 3.0
 */

package cc.hadoop.paccopt;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Initialization.
 * This class changes the format of an edge list file from a text format to a sequential format.
 * 
 * @author hmpark
 *
 */
public class InitByReverseUnionFind extends Configured implements Tool{

	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long outputSize;

	/**
	 * Transform a tab separated text file
	 * @param input file path.
	 * @param output file path.
	 * @param verbose true for verbose logging.
	 */
	public InitByReverseUnionFind(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
	}

	/**
	 * the main entry point
	 * @param args [0]: input file path, [1]: output file path, and tool runner arguments inherited from pacc
	 * @throws Exception of hadoop
	 */
	public static void main(String[] args) throws Exception{
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		ToolRunner.run(new InitByReverseUnionFind(input, output, true), args);
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters inherited from pacc
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf();
		
		Job job = new Job(conf, title);
		
		job.setJarByClass(this.getClass());
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(InitializationMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setNumReduceTasks(0);
		
		FileSystem.get(conf).delete(output, true);
		
		job.waitForCompletion(verbose);
		
		outputSize = job.getCounters().findCounter(Counter.MAP_OUTPUT_RECORDS).getValue();
		
		return 0;
	}

	static public class InitializationMapper extends Mapper<Object, Text, LongWritable, LongWritable>{

		Long2LongOpenHashMap parent;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			parent = new Long2LongOpenHashMap();
			parent.defaultReturnValue(-1);
		}

		private void union(long a, long b) {
			long r1 = find(a);
			long r2 = find(b);

			if(r1 < r2){
				parent.put(r1, r2);
			}
			else if(r1 > r2){
				parent.put(r2, r1);
			}
		}

		private long find(long x) {

			long p = parent.get(x);
			if(p != -1){
				long new_p = find(p);
				parent.put(x, new_p);
				return new_p;
			}
			else{
				return x;
			}

		}

		LongWritable ou = new LongWritable();
		LongWritable ov = new LongWritable();

		/**
		 * the map function of Initialization.
		 * @param key not used.
		 * @param value a line of the input text file.
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer st = new StringTokenizer(value.toString());

            long u = Long.parseLong(st.nextToken());
            long v = Long.parseLong(st.nextToken());

			if(find(u) != find(v)){
			    union(u, v);
            }
		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
		    for(Long2LongMap.Entry pair : parent.long2LongEntrySet()){
		        ou.set(pair.getLongKey());
		        ov.set(find(pair.getLongValue()));
		        context.write(ou, ov);
            }
        }
    }
	
}
