import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dictionary {
	
	public static class WordMapper extends Mapper<Text,Text,Text,Text>{
		private Text text = new Text();
		public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),",");
			while(tokenizer.hasMoreTokens()){
				text.set(tokenizer.nextToken());
				context.write(key, text);
			}
		}
	}
	
	public static class AllTranslationsReducer extends Reducer<Text,Text,Text,Text>{
		private Text result = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			String translations = "";
			for(Text txt : values)
				translations += "|" + txt.toString();
			result.set(translations);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"dictionary");
		job.setJarByClass(Dictionary.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(AllTranslationsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 :1);
	}
}
