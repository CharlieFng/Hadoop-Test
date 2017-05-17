package hbasetest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HbaseMR {

	
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "mapreduce on hbase");
		
		Scan scan = new Scan();
		scan.setCaching(1000);
		
		//TableMapReduceUtil.initTableMapJob("student","basicInfo" , MyMapper.class, Text.class, Text.class, job);
		//TableMapReduceUtil.initTableReduceJob("students_age", MyReducer.class, job);
		
		job.waitForCompletion(true);
		
		
	}

	
	
class MyMapper extends TableMapper<Text,Text> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		String k = key.toString();
		String v = value.getValue(Bytes.toBytes("basicInfo"), Bytes.toBytes("age")).toString();
		
		context.write(new Text(v), new Text(k));
		
	}
	
}


class MyReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		
		for(Text value : values){
			put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(value.toString()));
		}
		
		context.write(null, put);
		
	}
	

	
}

}
