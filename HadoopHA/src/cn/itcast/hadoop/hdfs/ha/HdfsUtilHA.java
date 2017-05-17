package cn.itcast.hadoop.hdfs.ha;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtilHA {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf, "fengsiyu");
		fs.copyFromLocalFile(new Path("/Users/fengsiyu/Documents/wc/src/LoveLetter.txt")
											, new Path("hdfs://ns1/"));
	}

}
