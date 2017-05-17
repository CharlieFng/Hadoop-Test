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
	
	
	
	//�ڷ����л�ʱ�����������Ҫ���ÿղι��캯����������ʾ������һ���ղι��캯��
	public FlowBean(){}
	
	
	//Ϊ�˶������ݵĳ�ʼ�����㣬����һ�����εĹ��캯��
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


	
	//���������л�������
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phoneNum);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
		
	}
	
	
	//�������з����г����������
	//���������ж��������ֶ�ʱ����������л�ʱ��˳�򱣳�һ��
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
