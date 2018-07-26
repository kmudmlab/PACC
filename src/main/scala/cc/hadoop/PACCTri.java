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
 * File: PACC.java
 * - pacc. It finds connected components in a graph.
 * Version: 3.0
 */

package cc.hadoop;

import cc.hadoop.paccopt.Finalization;
import cc.hadoop.paccopt.Initialization;
import cc.hadoop.paccopt.PALargeStarOpt;
import cc.hadoop.paccopt.PASmallStarOpt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class PACCTri extends Configured implements Tool{

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
		ToolRunner.run(new PACCTri(), args);
	}

	/**
	 * submit the hadoop job
	 * @param args
	 * [0]: input file path. [1]: output file path.
	 * Tool runner options:
	 * -D verbose if true, it prints log verbosely (default: false).
	 * -D mapred.reduce.tasks number of reduce tasks (default: 1).
	 * -D numPartitions the number of partitions (default: the number of reduce tasks).
	 * -D localThreshold pacc run an in-memory cc algorithm if the remaining number of edges is below
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
		long localThreshold = conf.getLong("localThreshold", 1000000);
		conf.setInt("numPartitions", numPartitions);
		conf.setLong("mapred.task.timeout", 0L);

		logger.info("Input                : " + input);
		logger.info("Output               : " + output);
		logger.info("Number of partitions : " + numPartitions);
		logger.info("Local threshold      : " + localThreshold);

		FileSystem fs = FileSystem.get(conf);
		
		fs.delete(output, true);
		
		long time = System.currentTimeMillis();
		long totalTime = time;



		Initialization init = new Initialization(input, output.suffix("_0/out"), verbose);
		
		ToolRunner.run(conf, init, null);

		logger.info("Round 0 (init) ends :\t" + ((System.currentTimeMillis() - time)/1000.0));

		PALargeStarOpt largeStar;
		PASmallStarOpt smallStar;
		
		long numEdges = init.outputSize;
		long numChanges;
		boolean converge;
		int i=0;
		
		do{

			if(numEdges > localThreshold){

				time = System.currentTimeMillis();

				largeStar = new PALargeStarOpt(output.suffix("_" + i + "/out"), output.suffix("_large_" + i), verbose); 
				ToolRunner.run(conf, largeStar, null);
				fs.delete(output.suffix("_" + i + "/out"), true);

				smallStar = new PASmallStarOpt(output.suffix("_large_" + i + "/out"), output.suffix("_" + (i + 1)), verbose);
				ToolRunner.run(conf, smallStar, null);
				fs.delete(output.suffix("_large_" + i + "/out"), true);

				logger.info(String.format("Round %d (star) ends :\tlout(%d)\tlcc(%d)\tlin(%d)\tsout(%d)\tsin(%d)\t%.2fs",
						i, largeStar.outSize, largeStar.ccSize, largeStar.inSize, smallStar.outSize, smallStar.inSize,
						((System.currentTimeMillis() - time) / 1000.0)));

				numChanges = largeStar.numChanges + smallStar.numChanges;
				numEdges = smallStar.outSize;
				
				converge = (numChanges == 0);
				
			}
			else{

				UnionFindJob lcc = new UnionFindJob(output.suffix("_" + i + "/out"), output.suffix("_" + (i+1) + "/out"));
				
				time = System.currentTimeMillis();
				
				ToolRunner.run(conf, lcc, null);

				logger.info(String.format("Round %d (local) ends :\tout(%d)\t%.2fs",
						i, lcc.outputSize, ((System.currentTimeMillis() - time) / 1000.0)));

				fs.delete(output.suffix("_" + i + "/out"), true);
				
				numEdges = lcc.outputSize;
				converge = true;
			}
			
			i++;
			
			
		}while(!converge && fs.exists(output.suffix("_" + i + "/out")));
		
		time = System.currentTimeMillis();
		
		Finalization fin = new Finalization(output, i, verbose);
		
		ToolRunner.run(conf, fin, null);

		logger.info(String.format("Round %d (final) ends :\t%.2fs",
				i, ((System.currentTimeMillis() - time) / 1000.0)));

		for(int r = 0; r <= i; r++){
			fs.delete(output.suffix("_"+r), true);
			fs.delete(output.suffix("_large_"+r), true);
		}
		
		System.out.print("[PACCOpt-end]\t" + input.getName() + "\t" + output.getName() + "\t" + numPartitions + "\t" + numReduceTasks + "\t" + localThreshold + "\t" + (i+1) + "\t");
		System.out.print( ((System.currentTimeMillis() - totalTime)/1000.0) + "\t" );
		System.out.println("# input output numPartitions numReduceTasks localThreshold numRounds time(sec)");
		
		
		return 0;
	}
}
