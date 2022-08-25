/**
* DB connection class
*
* @author Naveen Kumar
*/
package com.aws.americold.fetch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBconnection {
	private static String dbhost = "jdbc:mysql://americold-db.cxzaaggckjfa.us-east-2.rds.amazonaws.com:3306/americold";
	private static String username = "admin";
	private static String password = "americold";
	private static Connection conn;
	
	@SuppressWarnings("finally")
	public static Connection createNewDBconnection() {
		try  {	
			conn = DriverManager.getConnection(
					dbhost, username, password);	
		} catch (SQLException e) {
			System.out.println("Cannot create database connection");
			e.printStackTrace();
		} finally {
			return conn;	
		}		
	}
}
