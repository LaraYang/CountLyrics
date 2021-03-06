import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Count {
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		final static Pattern pattern = Pattern.compile("[\\w']+");
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				 word.set(matcher.group().toLowerCase());
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
		 public void reduce(Text key, Iterable<LongWritable> values,
	                Context context) throws IOException, InterruptedException {
			 long sum = 0L;
	         for (LongWritable value : values) {
	            sum += value.get();
	         }
	         context.write(key, new LongWritable(sum));
	       }
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJobName("Count Words");
		job.setJarByClass(Count.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
