package step4;


import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Step4MatrixMultiplication {

	public static class MatrixMultipleMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private final static Map<Integer, ArrayList<Co_concurrence>> map = new HashMap<Integer, ArrayList<Co_concurrence>>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] tokens = itr.nextToken().split("\\s+");
				String[] v1 = tokens[0].split(":");
				String[] v2 = tokens[1].split(":");

				// Co-concurrency Matrix
				if (v1.length > 1) {
					int items1 = Integer.valueOf(v1[0]);
					int items2 = Integer.valueOf(v1[1]);
					int num = Integer.valueOf(tokens[1]);
					ArrayList<Co_concurrence> list = null;
					if (!map.containsKey(items1)) {
						list = new ArrayList<Co_concurrence>();
					} else {
						list = map.get(items1);
					}
					Co_concurrence concurrence = new Co_concurrence(items1, items2, num);
					list.add(concurrence);
					map.put(items1, list);
				}

				// UserVector
				if (v2.length > 1) {
					int itemID = Integer.valueOf(tokens[0]);
					int userID = Integer.valueOf(v2[0]);
					double rating = Double.valueOf(v2[1]);
					for (Co_concurrence concurrence : map.get(itemID)) {
						int item = concurrence.getItemsID2();
						double result = concurrence.getNum() * rating;
						Text k = new Text(userID + " " + item);
						DoubleWritable v = new DoubleWritable(result);
						context.write(k, v);
					}
				}
			}
		}
	}

	public static class MatrixMultipleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double result = 0;
			for (DoubleWritable value : values) {
				result = result + Double.parseDouble(value.toString());
			}
			context.write(key, new DoubleWritable(result));
		}
	}

	public static void run(Map<String, String> path) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiple");
		job.setJarByClass(Step4MatrixMultiplication.class);
		job.setMapperClass(MatrixMultipleMapper.class);
		job.setReducerClass(MatrixMultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, new Path(path.get("Step3Output")), new Path(path.get("Step2Output")));
		FileOutputFormat.setOutputPath(job, new Path(path.get("Step4Output")));
		job.waitForCompletion(true);
	}
}
