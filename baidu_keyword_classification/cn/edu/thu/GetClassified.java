package cn.edu.thu;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @author zhf 
 * @email zhf.thu@gmail.com
 * @version 创建时间：2014年6月16日 下午2:45:32
 *  从原始数据中将有类标的数据抽取出来，并归并起来，每一类放于一行中，用\t分隔 
 *  最终格式如下：label\tkeywords
 */
public class GetClassified extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		Tool tool = new GetClassified();
		ToolRunner.run(tool, args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("out"),true);
		
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path("keyword_class.txt"));
		FileOutputFormat.setOutputPath(job, new Path("out"));
		
		job.setMapperClass(GetMapper.class);
		job.setReducerClass(GetReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class GetMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString().trim();
			if(line.length() > 0){
				String[] str = line.split("\t");
				//搜索词可能包含有空格或\t，故取数组中的最后一个数字作为label
				if(str.length>1 && isNumeric(str[str.length - 1].trim()))
					context.write(new Text(str[1]), new Text(str[0]));
			}
		}
	}
	
	public static class GetReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			StringBuffer bf = new StringBuffer();
			while(iter.hasNext()){
				bf.append(iter.next() + "\t");
				context.write(key, new Text(bf.toString()));
			}
		}
	}
	
	//label是1-33之间的数
	public static boolean isNumeric(String str){
		Pattern pattern = Pattern.compile("\\b[1-9]\\b|\\b[1-2][0-9]\\b|\\b[3][0-3]\\b"); 
		return pattern.matcher(str).matches(); 
	}
}

