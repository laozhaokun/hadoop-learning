package seg;

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

public class SegmentTool extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		Tool tool = new SegmentTool();
		ToolRunner.run(tool, args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"segment");
		FileSystem fs  = FileSystem.get(conf);
		fs.delete(new Path("/dmptest/user/zhaohf/output"),true);
		job.setJarByClass(SegMapper.class);
		job.setMapperClass(SegMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/dmptest/user/zhaohf/input"));
		FileOutputFormat.setOutputPath(job, new Path("/dmptest/user/zhaohf/output"));
		job.waitForCompletion(true);
		return 0;
	}
	public static class SegMapper extends Mapper<Object,Text,Text,Text> {
		private Text word = new Text();
		private Text ip = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String str[] = line.split("\t");
			StringBuffer buf = new StringBuffer();
			for(int i=1;i<str.length;i+=2){
				if(str[i].startsWith("http"))
					buf.append(str[i]);
				else
					buf.append(segment(str[i]));
			}
			word.set(buf.toString());
			context.write(ip,word);
		}
		public static String segment(String str) throws IOException{
			byte[] byt = str.getBytes();
			Reader reader = new InputStreamReader(new ByteArrayInputStream(byt));
			StringBuffer buf =  new StringBuffer();
			IKSegmenter ik = new IKSegmenter(reader,true);
			Lexeme lexeme = null;
			while((lexeme = ik.next()) != null){
				buf.append(lexeme.getLexemeText() +" ");
			}
			return buf.toString().trim();
		}
	}
}
