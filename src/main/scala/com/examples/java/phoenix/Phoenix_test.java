package com.examples.java.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Phoenix_test {

    public static void main(String[] args) throws SQLException {

        Statement stmt = null;

        Connection con = DriverManager.getConnection("jdbc:phoenix:43.247.90.151");

        stmt = con.createStatement();

        int n = 50;

// stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");

        Long a = System.currentTimeMillis();

        for (int i = 0; i < n; i++) {

            stmt.executeUpdate("upsert into test values (" + i * 5 + ",'Hello')");

        }

        con.commit();

        Long b = System.currentTimeMillis();

        System.out.println(b - a);

        con.close();

    }

}