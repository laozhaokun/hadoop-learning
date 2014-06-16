import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
public class  FileInfo
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 1){
			System.out.println("Usage FileInfo <target>");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(args[0]),conf);
		FileStatus fs = hdfs.getFileStatus(new Path(args[0]));
		System.out.println("path: "+fs.getPath());
		System.out.println("length: "+fs.getLen());
		System.out.println("modify time: "+fs.getModificationTime());
		System.out.println("owner: "+fs.getOwner());
		System.out.println("replication: "+fs.getReplication());
		System.out.println("blockSize: "+fs.getBlockSize());
		System.out.println("group: "+fs.getGroup());
		System.out.println("permission: "+fs.getPermission().toString());
	}
}
