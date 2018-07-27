package cc.hadoop.pacctri;

import cc.hadoop.Counters;
import cc.hadoop.utils.ExternalSorter;
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
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;


public class PALargeStarOptStep2 extends Configured implements Tool{

	private final Path input;
	private final Path output;
	private final String title;
	private final boolean verbose;
    public long numChanges;
    public long inputSize;
	public long outSize;
    public long ccSize;
    public long inSize;

	/**
	 * constructor
	 * @param input file path
	 * @param output file path
	 * @param verbose if true, it prints log verbosely.
	 */
	public PALargeStarOptStep2(Path input, Path output, boolean verbose){
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

		ToolRunner.run(new PALargeStarOptStep2(input, output, true), args);
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

		job.setMapperClass(Mapper.class);
		job.setCombinerClass(ColorLargeStarCombiner.class);
		job.setReducerClass(ColorLargeStarReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);


		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		FileSystem fs = FileSystem.get(getConf());

		fs.delete(output, true);


		if(fs.exists(input)){
			job.waitForCompletion(verbose);
			this.numChanges = job.getCounters().findCounter(Counters.NUM_CHANGES).getValue();
			this.inputSize = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			this.outSize = job.getCounters().findCounter(Counters.OUT_SIZE).getValue();
			this.ccSize = job.getCounters().findCounter(Counters.CC_SIZE).getValue();
			this.inSize = job.getCounters().findCounter(Counters.IN_SIZE).getValue();
		}

		return 0;
	}

    public static int part(long n, int p){
        return Long.hashCode(n) % p;
    }


    static public class ColorLargeStarCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        LongWritable ov = new LongWritable();

		/**
		 * the combiner function of LargeStarOpt
		 * @param _u source node
		 * @param values destination nodes
		 * @param context of hadoop
		 * @throws IOException by hadoop
		 * @throws InterruptedException by hadoop
		 */
        @Override
        protected void reduce(LongWritable _u, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long u = _u.get();

            long mu = Long.MAX_VALUE;

            for(LongWritable _v : values){

                long v = _v.get();

                if(v >= 0 && v < u){
                    mu = Math.min(mu, v);
                }
                else{
                    context.write(_u, _v);
                }
            }

            if(mu != Long.MAX_VALUE){
                ov.set(mu);
                context.write(_u, ov);
            }

        }
    }

	static public class ColorLargeStarReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		MultipleOutputs<LongWritable, LongWritable> mout;
		ExternalSorter sorter;

		/**
		 * setup before execution
		 * @param context hadoop context
		 */
		@Override
		protected void setup(Context context){
			String[] tmpPaths = context.getConfiguration().getTrimmedStrings("yarn.nodemanager.local-dirs");
			sorter = new ExternalSorter(tmpPaths);

			mout = new MultipleOutputs<>(context);
		}

		LongWritable om = new LongWritable();
		LongWritable ov = new LongWritable();


        class PredicateWithMin implements Predicate<Long> {

            long mu;
            long u;
            boolean is_star = true;

            PredicateWithMin(long u) {
                this.u = u;
                this.mu = u;
            }

            public boolean test(Long v) {

                if(v >= 0){
                    is_star = false;
                    mu = Math.min(mu, v);
                    return u < v;
                }
                else{
                	return true;
				}

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

		    long u = key.get();

			long numChanges = 0;
			long outSize = 0;
			long ccSize = 0;

            PredicateWithMin lfilter = new PredicateWithMin(u);

            Iterator<Long> it = StreamSupport.stream(values.spliterator(), false)
                    .map(LongWritable::get).filter(lfilter).iterator();

            Iterator<Long> uN_large = sorter.sort(it);

            long mu = lfilter.mu;
            boolean is_star = lfilter.is_star;

            om.set(mu);

            if(is_star){
                //Note: if is_star is true, every v is leaf.
                //Also, mu is u.
                //cc filtering.
                while(uN_large.hasNext()){
                    long v = uN_large.next();
                    ov.set(~v);
                    mout.write(ov, om, "final/part-step2");
                    ccSize++;
                }
            }
            else{

                boolean change = mu != u;

                while(uN_large.hasNext()){
                    long v = uN_large.next();
                    ov.set(v < 0 ? ~v : v);
                    mout.write(ov, om, "out/part-step2");
                    outSize++;
                    if(change) numChanges++;
                }
            }


			context.getCounter(Counters.NUM_CHANGES).increment(numChanges);
			context.getCounter(Counters.OUT_SIZE).increment(outSize);
            context.getCounter(Counters.CC_SIZE).increment(ccSize);


		}

        @Override
        protected void cleanup(
                Context context)
                throws IOException, InterruptedException {
            mout.close();
        }

	}


}
