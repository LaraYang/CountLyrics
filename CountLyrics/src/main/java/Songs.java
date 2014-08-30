import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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

import com.google.common.collect.Lists;

public class Songs {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		final static Pattern pattern = Pattern.compile("[\\w]+");
		final static String filePath = "/home/lyang/Documents/";
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String thisSong = value.toString();
			String fileName = filePath + thisSong +".txt";
			String fileOut = filePath + thisSong + "Counts";
			String[] args = new String[2];
			args[0] = fileName;
			args[1] = fileOut;
			try {
				Count.main(args);
				Scanner sc = new Scanner(new FileReader(fileOut + "/part-r-00000"));
				TreeMap<IntWritable, String> map = new TreeMap<IntWritable, String>(Collections.reverseOrder());
				while (sc.hasNext()) {
					String name = sc.next();
					IntWritable freq = new IntWritable(sc.nextInt());
					map.put(freq, name);
				}
				int count = 0;
				while (!map.isEmpty() && count < 5) {
					IntWritable TreeKey = map.firstKey();
					word.set(thisSong + ":" + map.get(TreeKey));
					context.write(word, TreeKey);
				    map.remove(TreeKey);
				    count += 1;
				}
				
			} catch (ClassNotFoundException e) {
				System.out.println("Class not Found in Mapper in Songs.java");
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		 public void reduce(Text key, Iterable<IntWritable> values,
	                Context context) throws IOException, InterruptedException {
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "Words");
        	String[] names = key.toString().split(":");
			Put p = new Put(Bytes.toBytes(names[0]));
			for (IntWritable value : values) {
				p.add(Bytes.toBytes("Counts"), Bytes.toBytes(names[1]), Bytes.toBytes(value.get()));
	    		table.put(p);
			 }
		 }
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJobName("Spread out songs");
		job.setJarByClass(Count.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/lyang/Documents/AllSongs.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/lyang/Documents/CountAllSongs"));

		job.waitForCompletion(true);
	}
}


