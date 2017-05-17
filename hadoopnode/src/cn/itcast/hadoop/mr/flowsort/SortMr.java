package cn.itcast.hadoop.mr.flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.hadoop.mr.flowsum.FlowBean;
import cn.itcast.hadoop.mr.flowsum.FlowSumMapper;
import cn.itcast.hadoop.mr.flowsum.FlowSumReducer;
import cn.itcast.hadoop.mr.flowsum.FlowSumRunner;

public class SortMr {
	
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		
		//拿到一行数据，切分出个字段，封装为一个flowbean，作为key输出
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields = StringUtils.split("\t");
			
			String phoneNum = fields[0];
			long up_flow = Long.parseLong(fields[1]);
			long down_flow = Long.parseLong(fields[2]);
			
			context.write(new FlowBean(phoneNum,up_flow,down_flow), NullWritable.get());
		}
	}
	
	
	public static class SortReducer extends Reducer<FlowBean,LongWritable,Text,FlowBean>{
		
		protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException{
			
			String phoneNum = key.getPhoneNum();
			context.write(new Text(phoneNum), key);
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SortMr.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputKeyClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
	
}
