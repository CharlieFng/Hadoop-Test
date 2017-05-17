package HadoopDemo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemCat {
	public static void main(String[] args) throws Exception{
		String uri = "hdfs://hadoopnode:9000/aaa/qingshu2.txt";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopnode:9000/");
		FileSystem fs = FileSystem.get(URI.create(uri), conf, "fengsiyu");
		FSDataInputStream in = null;
		try{
			in = fs.open(new Path(uri));
			IOUtils.copy(in, System.out);
			System.out.println();
			System.out.println("第二次访问-----------------------------：");
			in.seek(0);           //可以移动到文件中任意绝对位置
			IOUtils.copy(in, System.out);
		}finally{
			fs.close();
		}
		
		
	}
}
