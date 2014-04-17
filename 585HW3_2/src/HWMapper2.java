import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HWMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private class Reference{
		float x;
		float y;
	}
	private Hashtable<String, Reference> cities = null;
	
	
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		cities = new Hashtable<String, Reference>();
		try{
			Configuration conf = context.getConfiguration();
			FileSystem dfs = FileSystem.get(conf);
			Path src = new Path(dfs.getWorkingDirectory() + "/585HW3/Cities_Locations.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(dfs.open(src)));
			String line = null;
			while((line = br.readLine()) != null){
				String[] data = line.split(", ");
				float x = Float.parseFloat(data[2]);
				float y = Float.parseFloat(data[3]);
				Reference r = new Reference();
				r.x = x;
				r.y = y;
				cities.put(data[1], r);
			}
		}catch(Exception e){	
		}
	}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String record = value.toString();
		String[] data = record.split(", ");
		float x = Float.parseFloat(data[2]);
		float y = Float.parseFloat(data[3]);
		Reference r = cities.get(data[1]);
		if(r != null){
			float distanceSquared = (x - r.x) * (x - r.x) + (y - r.y) * (y - r.y);
			if(distanceSquared <= 25.0f){
				context.write(new Text(data[1]), one);
			}
		}
	}

}
