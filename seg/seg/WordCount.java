package seg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * @ClassName: WordCount 
 * @Description: TODO 
 * @author zhaohf@asiainfo.com 
 * @date 2014年8月15日 下午7:43:31 
 * 
 */
public class WordCount extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		Tool tool = new SegmentTool();
		ToolRunner.run(tool, args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"segment");
//		FileSystem fs  = FileSystem.get(conf);
//		fs.delete(new Path("/dmptest/user/zhaohf/output"),true);
		job.setJarByClass(SegMapper.class);
		job.setMapperClass(SegMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(SegReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/dmptest/user/zhaohf/output"));
		FileOutputFormat.setOutputPath(job, new Path("/dmptest/user/zhaohf/out"));
		job.waitForCompletion(true);
		return 0;
	}
	public static class SegMapper extends Mapper<Object,Text,Text,IntWritable> {
		private Text word = new Text();
		private IntWritable one = new IntWritable();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String str[] = line.split("\t");
			for(int i=1;i<str.length;i++){
				word.set(str[i]);
				context.write(word,one);
			}
		}
	}
	public static class SegReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values)
				sum += val.get();
			result.set(sum);
			context.write(key,result);
		}
	}
}
