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
 * ������ԭʼ��־��������ͳ�ƣ�����ͬʡ�ݵ��û�ͳ�ƽ���������ͬ���ļ� ��Ҫ�Զ�������������ƣ� 1.����������߼����Զ���һ��partiton
 * 2.�Զ���reducer task�Ĳ�����������
 * 
 * @author fengsiyu
 * 
 */
public class FlowSumArea {

	public static class FlowSumAreaMapper extends
			Mapper<LongWritable, Text, Text, FlowBean> {

		// �õ�һ�����ݣ��зֳ����ֶΣ���װΪһ��flowbean����Ϊkey���
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

		// ���ÿ����һ�����ݣ��͵���һ��reduce����
		// reduce�е�ҵ���߼����Ǳ���values��Ȼ������ۼ������
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
		
		//���������Զ���ķ����߼�
		job.setPartitionerClass(AreaPartitioner.class);
		
		//����reduce�����񲢷�����Ӧ�ø��������������һ��
		job.setNumReduceTasks(6);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}