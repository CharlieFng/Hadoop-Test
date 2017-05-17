package cn.itcast.hadoop.mr.areapartition;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.hadoop.mr.flowsum.FlowBean;




/**
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同的文件 需要自定义改造两个机制： 1.改造分区的逻辑：自定义一个partiton
 * 2.自定义reducer task的并发任务数量
 * 
 * @author fengsiyu
 * 
 */
public class FlowSumArea {

	public static class FlowSumAreaMapper extends
			Mapper<LongWritable, Text, Text, FlowBean> {

		// 拿到一行数据，切分出个字段，封装为一个flowbean，作为key输出
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = StringUtils.split("\t");

			String phoneNum = fields[0];
			long up_flow = Long.parseLong(fields[1]);
			long down_flow = Long.parseLong(fields[2]);

			context.write(new Text(phoneNum), new FlowBean(phoneNum, up_flow,
					down_flow));
		}
	}

	public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

		// 框架每传递一组数据，就调用一次reduce方法
		// reduce中的业务逻辑就是遍历values，然后进行累加再求和
		public void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {

			long up_flow_counter = 0;
			long down_flow_counter = 0;

			for (FlowBean bean : values) {
				up_flow_counter += bean.getUp_flow();
				down_flow_counter += bean.getDowm_flow();
			}

			context.write(key, new FlowBean(key.toString(), up_flow_counter,
					down_flow_counter));

		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumArea.class);
		
		job.setMapperClass(FlowSumAreaMapper.class);
		job.setReducerClass(FlowSumAreaReducer.class);
		
		//设置我们自定义的分组逻辑
		job.setPartitionerClass(AreaPartitioner.class);
		
		//设置reduce的任务并发数，应该跟分组的数量保持一致
		job.setNumReduceTasks(6);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
