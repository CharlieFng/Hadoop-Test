package cn.itcast.hivejdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class JDBCHive {
	
	
	private static String Driver = "org.apache.hadoop.hive.jdbc.HiveDriver" ;
	private static String URL = "jdbc:hive://hadoop01:10000/default";
	private static String name = "";
	private static String password = ""; 
	
	public static void main(String[] args) {
		
		try {
			Class.forName(Driver);
			Connection conn = DriverManager.getConnection(URL,name,password);
			Statement stat = conn.createStatement();
			String sql = "show tables";
			ResultSet rs = stat.executeQuery(sql);
			while(rs.next()){
				System.out.println(rs.getString(1));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
