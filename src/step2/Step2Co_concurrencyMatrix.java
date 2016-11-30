package step2;

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

public class Step2Co_concurrencyMatrix {
	public static class concurrencyMatrixMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text k = new Text();
		private IntWritable v = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] string = itr.nextToken().split("\\s+");
				String[] items = string[1].split(",");
				int size = items.length;

				for (int i = 0; i < size; i++) {
					String[] item1 = items[i].split(":");
					String itemID1 = item1[0];
					for (int j = 0; j < size; j++) {
						String[] item2 = items[j].split(":");
						String itemID2 = item2[0];
						k.set(itemID1 + ":" + itemID2);
						context.write(k, v);
					}
				}
			}
		}
	}

	public static class concurrencyMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "concurrencyMatrix");
		job.setJarByClass(Step2Co_concurrencyMatrix.class);
		job.setMapperClass(concurrencyMatrixMapper.class);
		job.setCombinerClass(concurrencyMatrixReducer.class);
		job.setReducerClass(concurrencyMatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.out.println(path.get("Step2Input"));
		FileInputFormat.addInputPath(job, new Path(path.get("Step2Input")));
		System.out.println(path.get("Step2Input"));
		FileOutputFormat.setOutputPath(job, new Path(path.get("Step2Output")));
		System.out.println(path.get("Step2Output"));
		job.waitForCompletion(true);

	}
}
