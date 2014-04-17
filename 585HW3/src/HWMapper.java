import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HWMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String record = value.toString();
		String[] data = record.split(",");
		context.write(new Text(data[0]), one);
	}
}
