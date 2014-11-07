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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * @ClassName: Horizontal2Vertical 
 * @Description: 横表转纵表
 * @author zhaohf@asiainfo.com 
 * @date 2014年8月27日 下午2:01:35 
 * 
 */
public class Horizontal2Vertical extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Horizontal2Vertical(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		String[] args = new GenericOptionsParser(arg0).getRemainingArgs();
		if(args.length != 2){
			System.err.println("Usage : TableTransferMR <input> <output>");
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]),true);
		Job job = new Job(conf);
		job.setJarByClass(Horizontal2Vertical.class);
		job.setMapperClass(TableMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0:1;
	}

	public static class TableMapper extends Mapper<LongWritable,Text,Text,Text>{
		public Text baseinfo = new Text();
		public Text filter = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString().trim();
			if(line == "")
				return;
			String[] params = line.split("\t");
			if(params.length < 10)
				return;
			String dspid = params[0];
			String token = params[1];
			String userseq = params[2];
			String ip = params[3];
			String iptime = params[4];
			String imei = params[5];
			String mac = params[6];
			String idfa = params[7];
			String filters = params[8];
			String platform = params[9];
			baseinfo.set(dspid+"\t"+token+"\t"+userseq+"\t"+ip+"\t"+iptime+"\t"+imei+"\t"+mac+"\t"+idfa);
			String[] fs = filters.split("\\|");
			for(String f : fs){
				filter.set(f+"\t"+platform);
				context.write(baseinfo, filter);
			}
		}
	}
}
