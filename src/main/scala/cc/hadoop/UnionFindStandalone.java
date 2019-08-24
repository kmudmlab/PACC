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
 * File: UnionFindJob.java
 * - The hadoop version of UnionFind. It finds connected components in a graph.
 */

package cc.hadoop;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.StringTokenizer;

public class UnionFindStandalone extends Configured implements Tool {

	private Long2LongOpenHashMap parent;


	private final Path input;
	private final Path output;
	public long outputSize = 0;


	public static void main(String[] args) throws Exception {
	    UnionFindStandalone job = new UnionFindStandalone(new Path(args[0]), new Path(args[1]));
        ToolRunner.run(job, args);
    }

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 */
	public UnionFindStandalone(Path input, Path output) {
		this.input = input;
		this.output = output;
		this.parent  = new Long2LongOpenHashMap();
		parent.defaultReturnValue(-1);
	}

	/**
	 * run union-find.
	 * @param args tool runner arguments
	 * @return not used
	 * @throws Exception by hadoop for sequence file I/O
	 */
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		fs.delete(output, true);

		LongWritable iKey = new LongWritable();
		LongWritable iValue = new LongWritable();

		FileStatus[] status;
		if(fs.isDirectory(input)){
			status = fs.listStatus(input, path -> path.getName().startsWith("part"));
		}
		else{
			status = new FileStatus[1];
			status[0] = fs.getFileStatus(input);
		}

		System.err.println("Status:");
		System.err.println(Arrays.toString(status));

		for (FileStatus statu : status) {

			System.err.println("Read " + statu.getPath());

			FSDataInputStream in = fs.open(statu.getPath());
			String line;
			while((line = in.readLine()) != null){
				StringTokenizer st = new StringTokenizer(line);
				long u = Long.parseLong(st.nextToken());
				long v = Long.parseLong(st.nextToken());

				union(u, v);
			}

		}


		OutputStreamWriter bw = new OutputStreamWriter(fs.create(output));

		for(java.util.Map.Entry<Long, Long> pair : parent.entrySet()){
			long u = pair.getKey();
			long v = find(u);

			bw.write(u + "\t" + v + "\n");
		}

		bw.close();

		return 0;
	}

	/**
	 * the find function of the union-find algorithm.
	 * parent table is updated as a side effect
	 * @param x node to process
	 * @return component id (smallest node reachable to node x)
	 */
	private long find(long x) {

		long p = parent.get(x);
		if(p != -1){
			long new_p = find(p);
			parent.put(x, new_p);
			return new_p;
		}
		else{
			parent.put(x, -1);
			return x;
		}

	}

	/**
	 * the union function of the union-find algorithm.
	 * It updates parent table to union the two
	 * @param a a node
	 * @param b another node
	 */
	private void union(long a, long b) {
		long r1 = find(a);
		long r2 = find(b);

		if(r1 > r2){
			parent.put(r1, r2);
		}
		else if(r1 < r2){
			parent.put(r2, r1);
		}
	}
}
