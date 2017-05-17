package com.itcast.hadoop.mr.topkurl;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopkURLReducer extends Reducer<Text,FlowBean,Text,LongWritable>{
	
	private TreeMap<FlowBean,Text> treeMap = new TreeMap<FlowBean,Text>();
	private double globalCount = 0;
	
	
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException{
		
		long up_sum = 0;
		long down_sum = 0;
		
		for(FlowBean bean : values){
			up_sum += bean.getUp_flow();
			down_sum += bean.getDowm_flow();
		}
		
		FlowBean tmpBean = new FlowBean("",up_sum,down_sum);
		
		treeMap.put(tmpBean, new Text(key.toString()));
		
		globalCount += tmpBean.getSum_flow();
		
	}
	
	//MapReduce框架中,当reduce任务方法全部执行完毕后，即将退出前，会调用一次cleanup方法
	protected void cleanup(Context context)
			throws IOException, InterruptedException{
		
		long tmpCount = 0;
		
		Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
		
		for(Entry<FlowBean,Text> entry : entrySet){
			
			if(tmpCount/globalCount < 0.8){
				
				context.write(entry.getValue(), new LongWritable(entry.getKey().getSum_flow()));
				
				tmpCount += entry.getKey().getSum_flow();
				
			}else{
				return;
			}
		}
	}
	
}
