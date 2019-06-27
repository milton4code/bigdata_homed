package com.examples.java.phoenix;

import java.sql.*;

public class Test_phoenix {

    public static void main(String[] args) throws SQLException {

        Statement stmt = null;

        ResultSet rset = null;

        int n = 50;

        Connection con = DriverManager.getConnection("jdbc:phoenix:master,slave1,slave2:2181");

        stmt = con.createStatement();

        stmt.executeUpdate("create table test22 (mykey integer not null primary key, mycolumn varchar)");

        Long a = System.currentTimeMillis();

        for (int i = 0; i <= n; i++) {

            stmt.executeUpdate("upsert into test22 values (" + i + ",'Hello')");

            con.commit();

        }

        Long b = System.currentTimeMillis();

        System.out.println(b - a);

        con.close();

    }

}