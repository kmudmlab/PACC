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
 * File: AltOpt.java
 * - The hadoop version of the optimized alternating algorithm introduced in the following paper:
 *   Raimondas Kiveris, Silvio Lattanzi, Vahab Mirrokni, Vibhor Rastogi, and Sergei Vassilvitskii. 2014.
 *   Connected Components in MapReduce and Beyond. SOCC, 2014
 */

package cc.hadoop;

import cc.hadoop.altopt.Finalization;
import cc.hadoop.altopt.Initialization;
import cc.hadoop.altopt.LargeStarOpt;
import cc.hadoop.altopt.SmallStarOpt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class AltOpt extends Configured implements Tool{

	private Logger logger = Logger.getLogger(getClass());

	/**
	 * the main entry point
	 * @param args
	 * [0]: input file path. [1]: output file path.
	 * Tool runner arguments:
	 * -D verbose if true, it prints log verbosely (default: false).
	 * -D mapred.reduce.tasks number of reduce tasks (default: 1).
	 * -D numPartitions the number of partitions (default: the number of reduce tasks).
	 * -D localThreshold pacc run a in-memory cc algorithm if the remaining number of edges is below
	 * this value (default: 1000000).
	 * @throws Exception by hadoop
	 */
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new AltOpt(), args);
	}

	/**
	 * submit the hadoop job
	 * @param args
	 * [0]: input file path. [1]: output file path.
	 * Tool runner options:
	 * -D verbose if true, it prints log verbosely (default: false).
	 * -D mapred.reduce.tasks number of reduce tasks (default: 1).
	 * -D numPartitions the number of partitions (default: the number of reduce tasks).
	 * this value (default: 1000000).
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf();
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		boolean verbose = conf.getBoolean("verbose", false);
		int numReduceTasks = conf.getInt("mapred.reduce.tasks", 1);
        int numPartitions = conf.getInt("numPartitions", numReduceTasks);
        conf.setInt("numPartitions", numPartitions);
		conf.setLong("mapred.task.timeout", 0L);

		logger.info("Input                : " + input);
		logger.info("Output               : " + output);
		logger.info("Number of partitions : " + numPartitions);

		FileSystem fs = FileSystem.get(conf);
		
		fs.delete(output, true);
		
		long time = System.currentTimeMillis();
		long totalTime = time;


		Initialization init = new Initialization(input, output.suffix("_0"), verbose);
		
		ToolRunner.run(conf, init, null);

		logger.info("Round 0 (init) ends :\t" + ((System.currentTimeMillis() - time)/1000.0));

		LargeStarOpt largeStar;
		SmallStarOpt smallStar;
		
		long numChanges;
		boolean converge;
		int round=0;
		
		do{

			time = System.currentTimeMillis();

			largeStar = new LargeStarOpt(output.suffix("_" + round), output.suffix("_large_" + round), verbose);
			ToolRunner.run(conf, largeStar, null);
			fs.delete(output.suffix("_" + round), true);

			smallStar = new SmallStarOpt(output.suffix("_large_" + round), output.suffix("_" + (round + 1)), verbose);
			ToolRunner.run(conf, smallStar, null);
			fs.delete(output.suffix("_large_" + round), true);

			logger.info(String.format("Round %d (star) ends :\tlout(%d)\tsout(%d)\tlchange(%d)\tschange(%d)\t%.2fs",
                    round, largeStar.outSize, smallStar.outSize, largeStar.numChanges, smallStar.numChanges,
                    ((System.currentTimeMillis() - time) / 1000.0)));

			numChanges = largeStar.numChanges + smallStar.numChanges;

			converge = (numChanges == 0);
				
			round++;
			
			
		}while(!converge && fs.exists(output.suffix("_" + round)));
		
		time = System.currentTimeMillis();

        Finalization fin = new Finalization(output.suffix("_" + round), output, verbose);
        ToolRunner.run(conf, fin, null);

        fs.delete(output.suffix("_" + round), true);

		logger.info(String.format("Round %d (final) ends :\t%.2fs",
				round, ((System.currentTimeMillis() - time) / 1000.0)));

		System.out.print("[AltOpt-end]\t" + input.getName() + "\t" + output.getName() + "\t" + numReduceTasks + "\t" + round + "\t");
		System.out.print( ((System.currentTimeMillis() - totalTime)/1000.0) + "\t" );
		System.out.println("# input output numReduceTasks numRounds time(sec)");
		
		
		return 0;
	}
}
