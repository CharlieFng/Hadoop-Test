package cn.itcast.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	//���ÿ����һ�����ݣ��͵���һ��reduce����
	//reduce�е�ҵ���߼����Ǳ���values��Ȼ������ۼ������ 
	public void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException{
		
		long up_flow_counter = 0;
		long down_flow_counter = 0;
		
		for(FlowBean bean : values){
			up_flow_counter += bean.getUp_flow();
			down_flow_counter += bean.getDowm_flow();
		}
		
		
		context.write(key, new FlowBean(key.toString(), up_flow_counter, down_flow_counter));
		
		
	}
}
