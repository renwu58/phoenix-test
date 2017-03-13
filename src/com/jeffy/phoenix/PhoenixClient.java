/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeffy.phoenix;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @Author Jeffy
 * @Email: renwu58@gmail.com
 * 
 */
public class PhoenixClient {
	
	public final static String PHOENIX_DRIVER="org.apache.phoenix.jdbc.PhoenixDriver";
	
	public final static String URL="jdbc:phoenix:10.1.226.16:2181:/hbase-unsecure";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		/*testPhoenixJDBC();
		
		try {
			testTenant();
		} catch (SQLException e) {
			e.printStackTrace();
		}*/
	    /*try {
            testMulitStatment();
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
	    try {
            testJdbcMetadata();
        } catch (SQLException e) {
            e.printStackTrace();
        }
	}
	
	public static void testJdbcMetadata() throws SQLException{
	    try {
            Class.forName(PHOENIX_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = null;
        try {
            con = DriverManager.getConnection(URL);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
	    
       DatabaseMetaData md = con.getMetaData();
       try (ResultSet rs = md.getTables(null, "DEMO%", "%", new String[] { "TABLE" })) {
           while (rs.next()) {
               String catalogName = rs.getString(1);
               String schemaName = rs.getString(2);
               String tableName = rs.getString(3);
               System.out.println("table: " + schemaName +"."+ tableName);
           }
       }
       
       Statement st = con.createStatement();
       try(ResultSet rs = st.executeQuery("select 1 from DEMO.AAAA")){
           ResultSetMetaData rmd = rs.getMetaData();
           int len = rmd.getColumnCount();
           for(int i=1; i<=len; i++){
               System.out.println("Column Index: " + i);
               System.out.println("Column Name: "+ rmd.getColumnName(i));
               System.out.println("Column Type: " + rmd.getColumnType(i));
               System.out.println("Table Name: " + rmd.getTableName(i));
               System.out.println("Schema Name: "+ rmd.getSchemaName(i));
               System.out.println("Column Precision: " + rmd.getPrecision(i));
               System.out.println("Column Scale: " + rmd.getScale(i));
               System.out.println("Column Nullable: " + rmd.isNullable(i));
           }
       }
       
       con.close();
	}
	
	public static void testPhoenixJDBC(){
		Statement stmt = null;
		ResultSet rset = null;
		try {
			Class.forName(PHOENIX_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = null;
		try {
			con = DriverManager.getConnection(URL);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			stmt = con.createStatement();
			System.out.println("==> create table");
			stmt.executeUpdate("create table if not exists test (mykey integer not null primary key, mycolumn varchar)");
			stmt.executeUpdate("create table if not exists allstarfull (playerID varchar not null,yearID char(4) not null,gameNum integer,gameID char(15),teamID char(10),lgID char(5),GP integer,startingPos integer, CONSTRAINT pk PRIMARY KEY ( playerID,yearID) )SALT_BUCKETS=3,MULTI_TENANT=true");
			stmt.executeUpdate("create table if not exists appearances (yearID char(4) not null,teamID char(10) not null,lgID char(5),playerID varchar not null,G_all integer ,GS integer ,G_batting integer,G_defense integer,G_p integer,G_c integer,G_1b integer,G_2b integer,G_3b integer,G_ss integer,G_lf integer,G_cf integer,G_rf integer,G_of integer,G_dh integer,G_ph integer,G_pr integer, CONSTRAINT pk PRIMARY KEY ( yearID,teamID,playerID) )SALT_BUCKETS=3,MULTI_TENANT=true");

			System.out.println("==> insert data");
			stmt.executeUpdate("upsert into test values (1,'Hello')");
			stmt.executeUpdate("upsert into test values (2,'World!')");
			con.commit();
			System.out.println("==> query data");
			PreparedStatement statement = con.prepareStatement("select * from test");
			rset = statement.executeQuery();
			while (rset.next()) {
				System.out.println(rset.getString("mycolumn"));
			}
			statement.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void testTenant() throws SQLException{

		Properties propsJeffy = new Properties();
		propsJeffy.setProperty("TenantId", "jeffy");
		Connection conJeffy = DriverManager.getConnection(URL, propsJeffy);

		Properties propsAllen = new Properties();
		propsAllen.setProperty("TenantId", "allen");
		Connection conAllen = DriverManager.getConnection(URL, propsAllen);
		Statement stmt = null;
		ResultSet rset = null;
		
		Statement stmtAllen = null;
		ResultSet rsetAllen = null;
		
		try {
			stmt = conJeffy.createStatement();
			System.out.println("==> insert Jeffy data");
			stmt.executeUpdate("upsert into allstarfull(yearID,gameNum,gameID,teamID,lgID,GP) values ( '1955',0,'NLS195507120','ML1','NL',1)");
			stmt.executeUpdate("upsert into allstarfull(yearID,gameNum,gameID,teamID,lgID,GP) values ( '1956',0,'ALS195607100','ML1','NL',1)");
			conJeffy.commit();
			
			stmtAllen = conAllen.createStatement();
			System.out.println("==> insert Allen data");
			stmtAllen.executeUpdate("upsert into allstarfull(yearID,gameNum,gameID,teamID,lgID,GP,startingPos) values ('1957',0,'NLS195707090','ML1','NL',1,9)");
			stmtAllen.executeUpdate("upsert into allstarfull(yearID,gameNum,gameID,teamID,lgID,GP,startingPos) values ('1958',0,'ALS195807080','ML1','NL',1,9)");
			stmtAllen.executeUpdate("upsert into allstarfull(yearID,gameNum,gameID,teamID,lgID,GP,startingPos) values ('1959',1,'NLS195907070','ML1','NL',1,9)");
			conAllen.commit();
			
			System.out.println("==> query Jeffy data");
			PreparedStatement statement = conJeffy.prepareStatement("select * from allstarfull");
			rset = statement.executeQuery();
			ResultSetMetaData metaData = rset.getMetaData();
			int len = metaData.getColumnCount();
			System.out.println("==>" + len);
			while (rset.next()) {
				for(int i=1;i<=len;i++){
					System.out.print(rset.getObject(i) + " ");
				}
				System.out.println();
			}

			statement.close();
			conJeffy.close();

			System.out.println("==> query Allen data");
			PreparedStatement statA = conAllen.prepareStatement("select * from allstarfull");
			rsetAllen = statA.executeQuery();
			ResultSetMetaData metaDataAllen = rsetAllen.getMetaData();
			int size = metaDataAllen.getColumnCount();
			System.out.println("==>" + size);
			while (rsetAllen.next()) {
				for(int i=1;i<=size;i++){
					System.out.print(rsetAllen.getObject(i) + " ");
				}
				System.out.println();
			}
			statA.close();
			conAllen.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void testMulitStatment() throws SQLException{
	    try {
            Class.forName(PHOENIX_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(URL);
        con.setAutoCommit(false);

        String sql1 = "upsert into demo.aaaa values (?,?)";
        String sql2 = "upsert into demo_ad.aaaa values (?,?)";
        PreparedStatement state1 = con.prepareStatement(sql1);
        PreparedStatement state2 = con.prepareStatement(sql2);
        
        int i=10;
        while (i>0) {
            System.out.println("===> execute " + i);
            state1.setInt(1, i);
            state1.setString(2, "ssssseeeeee"+i);
            state2.setInt(1, i);
            state2.setString(2, "xxxxxxddddddd"+i);
            state1.executeUpdate();
            state1.executeUpdate();
            i--;
        }
        con.commit();
	    
        state1.close();
        state2.close();
        con.close();
	}
}
