/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Alt.java
 * - The hadoop version of the alternating algorithm introduced in the following paper:
 *   Raimondas Kiveris, Silvio Lattanzi, Vahab Mirrokni, Vibhor Rastogi, and Sergei Vassilvitskii. 2014.
 *   Connected Components in MapReduce and Beyond. SOCC, 2014
 */

package cc.hadoop;

import cc.hadoop.alt.Finalization;
import cc.hadoop.alt.Initialization;
import cc.hadoop.alt.LargeStar;
import cc.hadoop.alt.SmallStar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Alt extends Configured implements Tool{

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
		ToolRunner.run(new Alt(), args);
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
		conf.setLong("mapred.task.timeout", 0L);

		logger.info("Input                : " + input);
		logger.info("Output               : " + output);

		FileSystem fs = FileSystem.get(conf);
		
		fs.delete(output, true);
		
		long time = System.currentTimeMillis();
		long totalTime = time;



		Initialization init = new Initialization(input, output.suffix("_0"), verbose);
		
		ToolRunner.run(conf, init, null);

		logger.info("Round 0 (init) ends :\t" + ((System.currentTimeMillis() - time)/1000.0));

		LargeStar largeStar;
		SmallStar smallStar;
		
		long numChanges;
		boolean converge;
		int round=0;
		
		do{

			time = System.currentTimeMillis();

			largeStar = new LargeStar(output.suffix("_" + round), output.suffix("_large_" + round), verbose);
			ToolRunner.run(conf, largeStar, null);
			fs.delete(output.suffix("_" + round), true);

			smallStar = new SmallStar(output.suffix("_large_" + round), output.suffix("_" + (round + 1)), verbose);
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

		System.out.print("[Alt-end]\t" + input.getName() + "\t" + output.getName() + "\t" + numReduceTasks + "\t" + round + "\t");
		System.out.print( ((System.currentTimeMillis() - totalTime)/1000.0) + "\t" );
		System.out.println("# input output numReduceTasks numRounds time(sec)");
		
		
		return 0;
	}
}
