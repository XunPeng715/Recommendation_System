package step1;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1RatingMatrix {
	public static class RatingMatrixMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String[] string = itr.nextToken().split("::");
				int userID = Integer.valueOf(string[0]);
				String rating = string[1] + ":" + string[2];
				context.write(new IntWritable(userID), new Text(rating));
			}
		}
	}

	public static class RatingMatrixReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String result = null;
			for (Text val : values) {
				if (result == null) {
					result = val.toString();
				} else {
					result = result + "," + val.toString();
				}
			}
			context.write(key, new Text(result));
		}
	}
	
	public static void run(Map<String, String> path) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "rating matrix");
		job.setJarByClass(Step1RatingMatrix.class);
		job.setMapperClass(RatingMatrixMapper.class);
		job.setCombinerClass(RatingMatrixReducer.class);
		job.setReducerClass(RatingMatrixReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(path.get("Step1Input")));
		FileOutputFormat.setOutputPath(job, new Path(path.get("Step1Output")));
		job.waitForCompletion(true);
	}

}
