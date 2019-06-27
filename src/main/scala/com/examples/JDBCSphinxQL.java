package com.examples;

import java.sql.*;
public class JDBCSphinxQL {
public static void main(String[] argv) {
	try {
		Class.forName("com.mysql.jdbc.Driver");
	} catch (ClassNotFoundException e) {
		System.out.println("JDBC Driver not found!");
		e.printStackTrace();
		return;
	}
	Connection conn = null;
	Statement st = null;
	ResultSet rs = null;
	try {
	    String url = "jdbc:mysql://192.168.35.102:9306?user=&password=&useUnicode=true&characterEncoding=utf8";
        System.out.println(" 进入连接....");
	    conn = DriverManager.getConnection(url);
        System.out.println("conn =="+ conn);



	} catch (SQLException e) {
		System.out.println("Connection Failed! Check output console");
		e.printStackTrace();
		return;
	}
	try {
		st = conn.createStatement();
		System.out.println("Searching:");
		rs = st.executeQuery("SELECT * FROM tm_m_event_epg ");
		while (rs.next()) {
//			System.out.print(" id=" + rs.getString("id") + " gid="
//					+ rs.getString("gid") + " latitude="
//					+ rs.getString("latitude") + " longitude="
//					+ rs.getString("longitude")
//                                              );
//			System.out.println();
		}
		// optional, to get total count and other stats
		System.out.println("Stats:");
//		rs = st.executeQuery("SHOW META");
		while (rs.next()) {
			System.out.println(rs.getString(1) + ' ' + rs.getString(2));
		}
		// a simple insert 
//		st.executeUpdate("INSERT INTO testrt(id,title,content,gid,latitude,longitude)"
//				+ " VALUES(104,'some title','some description',12345,32.1343,45.56534)");
		rs.close();
		st.close();
	} catch (SQLException e) {
		System.out.println("Queries failed!");
		e.printStackTrace();
		return;
	} finally {
		try {
	           if(conn!=null)
	               conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
	}
}
}