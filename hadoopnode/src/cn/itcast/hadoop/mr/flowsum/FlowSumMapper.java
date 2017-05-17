 package cn.itcast.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * FlowBean �������Զ����һ���������ͣ�Ҫ��Hadoop�ĸ��ڵ�֮�䴫�䣬Ӧ����ѭHadoop�����л�����
 * �ͱ���ʵ��Hadoop��Ӧ�����л��ӿ�
 * @author fengsiyu
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	//�õ���־�е�һ�����ݣ��зָ����ֶΣ���ȡ����Ҫ���ֶ�
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		
		String line = value.toString();
		
		String[] fields = StringUtils.split(line,"\t");
		
		String phoneNum = fields[1];
		long up_flow = Long.parseLong(fields[7]);
		long down_flow = Long.parseLong(fields[8]);
		
		context.write(new Text(phoneNum), new FlowBean(phoneNum,up_flow,down_flow));
		
		
	}
	
	
	
	
	
}
