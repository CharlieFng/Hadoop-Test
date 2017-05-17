package HadoopDemo;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * ��ʾHadoop�ļ�ϵͳ��һ��·�����ļ���Ϣ
 * @author fengsiyu
 *
 */
public class ListStatus {
	
	public static void main(String[] args) throws IOException, InterruptedException{
		String uri = "hdfs://hadoopnode:9000/";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopnode:9000/");
		FileSystem fs = FileSystem.get(URI.create(uri), conf, "fengsiyu");
		
		Path[] paths = new Path[2];
		paths[0] = new Path(uri);
		paths[1] = new Path("/aaa");
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);  //��FileStatus��������ת��ΪPath��������
		for(Path p : listedPaths){
			System.out.println(p);
		}
				
	}
}
