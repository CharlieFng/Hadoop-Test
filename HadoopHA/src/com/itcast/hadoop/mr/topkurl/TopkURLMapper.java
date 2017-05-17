package com.itcast.hadoop.mr.topkurl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TopkURLMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		
		String line = value.toString();

		String[] fields = StringUtils.split(line, "\t");
		try {
			if (fields.length > 32 && StringUtils.isNotEmpty(fields[26])
					&& fields[26].startsWith("http")) {
				String url = fields[26];

				long up_flow = Long.parseLong(fields[30]);
				long d_flow = Long.parseLong(fields[31]);

				FlowBean bean = new FlowBean("", up_flow, d_flow);

				context.write(new Text(url), bean);
			}
		} catch (Exception e) {

			System.out.println();

		}
		
	}
}
