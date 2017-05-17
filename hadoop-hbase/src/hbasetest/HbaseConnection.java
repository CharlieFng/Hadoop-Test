package hbasetest;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseConnection {
	//private String rootDir;
	private String zkServer;
	//private String port;
	
	private Configuration conf;
	private HConnection conn = null;
	
	public HbaseConnection(String zkServer) throws Exception{
		//this.rootDir = rootDir;
		this.zkServer = zkServer;
		//this.port = port;
		
		conf = HBaseConfiguration.create();
		//conf.set("hbase.rootdir", rootDir);
		conf.set("hbase.zookeeper.quorum", zkServer);
		//conf.set("hbase.zookeeper.property.clientPort", port);
		conn = HConnectionManager.createConnection(conf);
	}
	
	
	
	public void createTable(String tableName, List<String> cols) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for(String col : cols){
			HColumnDescriptor colDesc = new HColumnDescriptor(col);
			colDesc.setCompressionType(Algorithm.GZ);
			colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
			tableDesc.addFamily(colDesc);
		}
		
		admin.createTable(tableDesc);
	}
	
	
	public void saveData(String tableName, List<Put> puts) throws Exception{
		   HTableInterface tblInterface = conn.getTable(tableName);
		   tblInterface.put(puts);
		   tblInterface.setAutoFlush(false);
		   tblInterface.flushCommits();
	}
	
	
	public Result getData(String tableName, String rowkey) throws Exception{
		HTableInterface tblInterface = conn.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowkey));
		return tblInterface.get(get);
	}
	
	
	public void format(Result result){
		String rowkey = Bytes.toString(result.getRow());
		KeyValue[] kvs = result.raw();
		for(KeyValue kv: kvs){
			String family = Bytes.toString(kv.getFamily());
			String qualifier = Bytes.toString(kv.getQualifier());
			String value = Bytes.toString(kv.getValue());
			System.out.println("rwokey->" + rowkey + "  family->" + family
								+ "  qualifier->" + qualifier + "  value->" + value);
			
		}
		
	}
	
	
	public void hbaseScan(String tableName) throws Exception {
		Scan scan = new Scan();
		scan.setCaching(1000); //设置缓存记录大小
		HTableInterface tblInterface = conn.getTable(tableName);
		ResultScanner scanner = tblInterface.getScanner(scan);
		
		for(Result res : scanner) {
			format(res);
		}
		
	}
	
	
	public void filterTest(String tableName) throws Exception{
		Scan scan = new Scan();
		scan.setCaching(1000); //设置缓存记录大小
		
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL
							, new RegexStringComparator("f\\w+"));
		
		scan.setFilter(filter);
		
		HTableInterface tblInterface = conn.getTable(tableName);
		ResultScanner scanner = tblInterface.getScanner(scan);
		
		for(Result res : scanner) {
			format(res);
		}
	}
	
	
	public void pageFilterTest(String tableName) throws Exception {
		PageFilter filter = new PageFilter(4);
		byte[] lastRow = null;
		int pageCount = 0;
		
		HTableInterface tblInterface = conn.getTable(tableName);
		
		while(++pageCount > 0){
			System.out.println("pageCount = " + pageCount);
			Scan scan = new Scan();
			scan.setFilter(filter);
			
			if(lastRow != null){
				scan.setStartRow(lastRow);
			}
			
			ResultScanner scanner = tblInterface.getScanner(scan);
			int count = 0;
			for(Result res : scanner) {
				lastRow = res.getRow();
				if(++count > 3) break;
				format(res);
				
			}
			if(count < 3) break;
		}
	}
	
	public static void main(String[] args) throws Exception{
		String zkServer = "hadoop05:2181,hadoop06:2181,hadoop07:2181";
		
		HbaseConnection conn = new HbaseConnection(zkServer);
		
		//建表
		/*
		List<String> cols = new LinkedList<String>();
		cols.add("basicInfo");
		cols.add("moreInfo");
		
		conn.createTable("student", cols);
		*/
		
		//向表中插入数据
		/*
		List<Put> puts = new LinkedList<Put>();
		
		Put put1 = new Put(Bytes.toBytes("fengsiyu"));
		put1.add(Bytes.toBytes("basicInfo"), Bytes.toBytes("age"),Bytes.toBytes("24"));
		put1.add(Bytes.toBytes("moreInfo"), Bytes.toBytes("phone"),Bytes.toBytes("010..."));
		
		Put put2 = new Put(Bytes.toBytes("zhangxiaoling"));
		put2.add(Bytes.toBytes("basicInfo"), Bytes.toBytes("age"),Bytes.toBytes("23"));
		put2.add(Bytes.toBytes("moreInfo"), Bytes.toBytes("phone"),Bytes.toBytes("031..."));
		
		puts.add(put1);
		puts.add(put2);
		
		conn.saveData("student", puts);
		*/
		
		//表的查询
		/*
		Result result = conn.getData("student", "fengsiyu");
		conn.format(result);  
		*/
		
		/*
		//通过扫描器查询数据
		conn.hbaseScan("student");
		*/
		
		
		//过滤器扫描
		conn.filterTest("student");
		//conn.pageFilterTest("student");
		
		
		
	}
}
