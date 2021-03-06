import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CountWord
{
	public static class CountMap 
		extends MapReduceBase 
		implements Mapper<LongWritable,Text,Text,IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key,Text value,
			OutputCollector<Text,IntWritable> output,
			Reporter reporter) throws IOException
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				output.collect(word,one);
			}
		}
	}

	public static class Reduce 
		extends MapReduceBase 
		implements Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output,Reporter reporter)
			throws IOException
		{
			int sum = 0;
			while(values.hasNext())
				sum += values.next().get();
			output.collect(key,new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException{
		JobConf job = new JobConf(CountWord.class);

		job.setJobName("countword");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(CountMap.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		JobClient.runJob(job);
	}
}  