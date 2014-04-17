import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HWDriver2 {

	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.err.println("Input argument not correct!");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "585HW3Part2");
		job.setJarByClass(HWDriver2.class);
		job.setMapperClass(HWMapper2.class);

		job.setReducerClass(HWReducer2.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true))
			return;
	}

}
