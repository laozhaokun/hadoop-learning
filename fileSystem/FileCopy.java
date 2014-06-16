import java.net.URI; 
import java.io.InputStream; 
import java.io.OutputStream; 
import java.io.BufferedInputStream; 
import java.io.FileInputStream; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.FSDataInputStream; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IOUtils;
public class FileCopy 
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 2){
		System.err.println("Usage : filecopy <source> <target>");
		System.exit(2);
		}
		Configuration conf = new Configuration();
		InputStream input = new BufferedInputStream(new FileInputStream(args[0]));
		FileSystem fs = FileSystem.get(URI.create(args[1]),conf);
		OutputStream output = fs.create(new Path(args[1]));
		IOUtils.copyBytes(input,output,4096,true);
	}

}
