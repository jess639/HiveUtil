package com.yiban.datacenter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class testHiveUtil {
	public static void main(String[] args) {

		String url = "jdbc:hive2://192.168.27.233:10000/default";
		String username = "hive";
		String password = "";

		try {

			HiveUtil hu = new HiveUtil(url, username, password);

			Connection conn = hu.getConnection();

			String QueryString = "select * from jn1";

			ResultSet rs = hu.excuteQuery(conn, QueryString);

			hu.printResultSet(rs);

			//drop and create table
			String tableName = "testHiveDriverTable";// "temp2";
			String DropTableSql="drop table if exists " + tableName;
			hu.excuteHql(conn, DropTableSql);
			
			String CreateTableSql="create table " + tableName + " (key int, value string)";
			hu.excuteHql(conn, CreateTableSql);
			
			// show tables
			String sql = "show tables '" + tableName + "'";
			ResultSet res = hu.excuteQuery(conn, sql);
			System.out.println("show tables:");
			hu.printResultSet(res);
			
			// describe table
			sql = "describe " + tableName;
			res = hu.excuteQuery(conn, sql);
			System.out.println("desc table:");
			hu.printResultSet(res);
			
			// load data into table
			// NOTE: filepath has to be local to the hive server
			// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per
			// line
			String filepath = "/tmp/a.txt";
			sql = "load data local inpath '" + filepath
					+ "' OVERWRITE into table " + tableName;
			System.out.println(sql);
			hu.excuteHql(conn, sql);

			// select * query 
			sql = "select * from " + tableName;
			res = hu.excuteQuery(conn, sql);
			hu.printResultSet(res);
			
			// regular hive query
			sql = "select count(*) from " + tableName;
			res = hu.excuteQuery(conn, sql);
			hu.printResultSet(res);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
