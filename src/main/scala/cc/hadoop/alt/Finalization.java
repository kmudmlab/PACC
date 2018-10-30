/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Finalization.java
 * - The finalization for the alternating algorithm. It just changes the format of graph file
 * from a sequence file format to a text file format
 */

package cc.hadoop.alt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Finalization.
 * This class changes the format of an edge list file from a sequence file format to a text file format.
 */
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

		job.setMapperClass(Mapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem.get(getConf()).delete(output, true);
		
		job.waitForCompletion(verbose);
		
		inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		
		return 0;
	}

}
