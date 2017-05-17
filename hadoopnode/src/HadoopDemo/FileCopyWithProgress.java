package HadoopDemo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	
	public static void main(String[] args) throws Exception{
		String localSrc = "/Users/fengsiyu/Documents/LoveLetter.txt";
		String dest = "/aaa/bbb/ccc/LoveLetter.txt";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopnode:9000/");
		FileSystem fs = FileSystem.get(URI.create(dest), conf, "fengsiyu");
		FSDataOutputStream out = fs.create(new Path(dest), new Progressable(){
			public void progress(){         //每次Hadoop调用progress()方法时——也就是每次将
				System.out.print(".");    //64KB数据包写入datanode管线后,打印出一个时间点来显示整个运行过程。
			}
		});
		
		
		IOUtils.copy(in, out);
	}
}
