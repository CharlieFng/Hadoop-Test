package cn.itcast.hadoop.mr.inverseindex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import cn.itcast.hadoop.mr.areapartition.AreaPartitioner;
import cn.itcast.hadoop.mr.areapartition.FlowSumArea;
import cn.itcast.hadoop.mr.areapartition.FlowSumArea.FlowSumAreaMapper;
import cn.itcast.hadoop.mr.areapartition.FlowSumArea.FlowSumAreaReducer;
/**
 * 倒排索引步骤一job
 * @author fengsiyu
 *
 */
public class InverseIndexStepOne {
	
	public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] fields = StringUtils.split(line, " ");
			
			//获取数据所在的文件切片
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//从文件切片中获取文件名
			String fileName = inputSplit.getPath().getName();
			
			for(String field:fields){
				context.write(new Text(field+"-->"+fileName), new LongWritable(1));
			}
		}
		
		
	}
	
	
	public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException{
			
			long counter = 0;
			for(LongWritable value:values){
				counter += value.get();
			}
			
			context.write(key, new LongWritable(counter));
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.jar", "ii.jar");
		
		Job job = Job.getInstance(conf);
	
		job.setJarByClass(InverseIndexStepOne.class);
		
		job.setMapperClass(StepOneMapper.class);
		job.setReducerClass(StepOneReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoopnode:9000/ii/data"));
		
		//检查一下参数所指定的输出路径是否存在，如果已存在，先删除
		Path output = new Path("hdfs://hadoopnode:9000/ii/stepone");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)){
			fs.delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
