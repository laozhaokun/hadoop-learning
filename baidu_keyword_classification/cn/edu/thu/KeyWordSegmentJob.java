package cn.edu.thu;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * @author zhf 
 * @email zhf.thu@gmail.com
 * @version 创建时间：2014年6月16日 上午9:59:14
 */
public class KeyWordSegmentJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		Tool tool = new KeyWordSegmentJob();
		ToolRunner.run(tool, args);
	}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("output"),true);
		job.setJarByClass(getClass());
		job.setMapperClass(SegMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("keyword_classified.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/labeld_segment.txt"));
		job.waitForCompletion(true);
		return 0;
	}

	public static class SegMapper extends Mapper<Text,Text,Text,Text>{
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] str = line.split("\t");
			if(str.length == 2){
				String keyword = str[0];
				String label = str[1];
				context.write(new Text(label), new Text(segment(keyword)));
			}
		}
	}
	
	//庖丁分词
	public static String segment(String str) throws IOException {
		byte[] byt = str.getBytes();
		Reader reader = new InputStreamReader(new ByteArrayInputStream(byt));
		StringBuffer buffer = new StringBuffer();
		IKSegmenter ik = new IKSegmenter(reader,true);
		Lexeme lexeme = null;//词元对象
		while((lexeme = ik.next()) != null)
			buffer.append(lexeme + " ");
		return buffer.toString();
	}
}
