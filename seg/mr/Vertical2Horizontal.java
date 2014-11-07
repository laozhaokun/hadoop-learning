package mr;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * @ClassName: TableTransferMR 
 * @Description: 纵表转横表
 * @author zhaohf@asiainfo.com 
 * @date 2014年8月19日 下午6:51:23 
 * 
 */
public class Vertical2Horizontal extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Vertical2Horizontal(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		String[] args = new GenericOptionsParser(arg0).getRemainingArgs();
		if(args.length != 2){
			System.err.println("Usage : Vertical2Horizontal <input> <output>");
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]),true);
		Job job = new Job(conf);
		job.setJarByClass(Vertical2Horizontal.class);
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0:1;
	}

	public static class TableMapper extends Mapper<LongWritable,Text,Text,Text>{
		public Text userSeq = new Text();
		public Text val = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString().trim();
			if(line != ""){
				String[] params = line.split("\t");
				String status = params[0];
				String userseq = params[1];
				String tag_index = params[2];
				String weight = params[3];
				userSeq.set(status + "\t" + userseq);
				val.set(tag_index+ ","+ weight);
				context.write(userSeq, val);
			}
		}
	}
	public static class TableReducer extends Reducer<Text,Text,Text,Text>{
		private Text result = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String str = "";
			for(Text val : values){
				String[] params = val.toString().split(",");
				if(!str.contains(params[0]))
					str += val + ";";
			}
			result.set(str.substring(0, str.length()-1));
			context.write(key, result);
		}
	}
}
