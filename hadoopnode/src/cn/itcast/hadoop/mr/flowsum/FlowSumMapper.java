 package cn.itcast.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * FlowBean 是我们自定义的一种数据类型，要在Hadoop的各节点之间传输，应该遵循Hadoop的序列化机制
 * 就必须实现Hadoop相应的序列化接口
 * @author fengsiyu
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	//拿到日志中的一行数据，切分各个字段，抽取出需要的字段
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
