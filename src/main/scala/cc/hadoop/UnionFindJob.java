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
 * File: UnionFind.java
 * - the union-find algorithm for finding connected components in a graph.
 * Version: 3.0
 */

package cc.hadoop;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class UnionFindJob extends Configured implements Tool {

	private Long2LongOpenHashMap parent;


	private final Path input;
	private final Path output;
	public long outputSize = 0;


	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 */
	public UnionFindJob(Path input, Path output) {
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

		FileStatus[] status = fs.listStatus(input, path -> path.getName().startsWith("part"));

		for (FileStatus statu : status) {
			try {
				SequenceFile.Reader sr = new SequenceFile.Reader(fs, statu.getPath(), conf);

				while (sr.next(iKey, iValue)) {
					long u = iKey.get();
					long v = iValue.get();
					if (u < 0) u = ~u;

					if (find(u) != find(v)) {
						union(u, v);
					}
				}
				sr.close();

			} catch (Exception ignored) {}
		}

		final SequenceFile.Writer out = new SequenceFile.Writer(fs, conf, output,
				LongWritable.class, LongWritable.class);

		final LongWritable ou = new LongWritable();
		final LongWritable ov = new LongWritable();

		parent.forEach((u, x) -> {
            try{
                long v = find(u);
                if(u != v){
                    ou.set(u);
                    ov.set(v);
                    out.append(ou, ov);
                    outputSize++;
                }
            } catch (IOException ignored){}
        });


		out.close();

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
