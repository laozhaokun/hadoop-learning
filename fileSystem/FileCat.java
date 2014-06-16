import java.net.URI;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class FileCat 
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 1){
			System.out.println("Usage FileCat <target>");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(args[0]),conf);
		InputStream in = null;
		try{
			in = hdfs.open(new Path(args[0]));
			IOUtils.copyBytes(in,System.out,4096,true);
		}finally{
			IOUtils.closeStream(in);
		}
	}
}
