/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * -------------------------------------------------------------------------
 * File: Initialization.java
 * - The initialization for the alternating algorithm.
 */

package cc.hadoop.altopt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
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
public class Initialization extends Configured implements Tool{
	
	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
	public long outputSize;

	/**
	 * constructor
	 * @param input file path.
	 * @param output file path.
	 * @param verbose true for verbose logging.
	 */
	public Initialization(Path input, Path output, boolean verbose){
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		this.title = String.format("[%s]%s", this.getClass().getSimpleName(), input.getName());
	}

	/**
	 * the main entry point
	 * @param args [0]: input file path, [1]: output file path, and tool runner arguments
	 * @throws Exception of hadoop
	 */
	public static void main(String[] args) throws Exception{
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		ToolRunner.run(new Initialization(input, output, true), args);
	}

	/**
	 * submit the hadoop job
	 * @param args tool runner parameters
	 * @return not used
	 * @throws Exception by hadoop
	 */
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf();


		Job job = Job.getInstance(conf, title);
		
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
		
		outputSize = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
		
		return 0;
	}

	static public class InitializationMapper extends Mapper<Object, Text, LongWritable, LongWritable>{

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
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException{
			try{
				StringTokenizer st = new StringTokenizer(value.toString());

				long u = CopyUtil.toNode(Long.parseLong(st.nextToken()));
                long v = CopyUtil.toNode(Long.parseLong(st.nextToken()));

				ou.set(u);
				ov.set(v);
				context.write(ou, ov);
			} catch (Exception e){
				e.printStackTrace();
			}
			
		}
		
	}
	
}
