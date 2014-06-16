import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
public class  FilesList
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 1){
			System.out.println("Usage : FilesList <target>");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(args[0]),conf);
		FileStatus[] fs = hdfs.listStatus(new Path(args[0]));
		Path[] listPath = FileUtil.stat2Paths(fs);
		for(Path p : listPath)
			System.out.println(p);
	}
}
