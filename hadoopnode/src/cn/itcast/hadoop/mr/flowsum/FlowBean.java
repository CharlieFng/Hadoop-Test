package cn.itcast.hadoop.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	
	private String phoneNum;
	private long up_flow;
	private long down_flow;
	private long sum_flow;
	
	
	
	//在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
	public FlowBean(){}
	
	
	//为了对象数据的初始化方便，加入一个带参的构造函数
	public FlowBean(String phoneNum, long up_flow, long down_flow){
		this.phoneNum = phoneNum;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow + down_flow;
		
	}
	
	public String getPhoneNum() {
		return phoneNum;
	}


	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
	}


	public long getUp_flow() {
		return up_flow;
	}


	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}


	public long getDowm_flow() {
		return down_flow;
	}


	public void setDowm_flow(long dowm_flow) {
		this.down_flow = dowm_flow;
	}


	public long getSum_flow() {
		return sum_flow;
	}


	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}


	
	//将对象序列化到流中
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phoneNum);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
		
	}
	
	
	//从数据中反序列出对象的数据
	//从数据流中读出对象字段时，必须跟序列化时的顺序保持一致
	public void readFields(DataInput in) throws IOException {
		
		phoneNum = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
		
	}
	
	
	public String toString(){
		return ""+ up_flow + "\t" + down_flow + "\t" + sum_flow;
	}


	public int compareTo(FlowBean bean) {
		
		return this.sum_flow>bean.getSum_flow() ? -1 : 1;
	}
	
	
	
}
