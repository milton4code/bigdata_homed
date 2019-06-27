package com.examples.java.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;


public class TestPhoenix {


    public static void main(String[] args) {
        PhoenixConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;


        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:master,slave1,slave2:2181");
            conn.setAutoCommit(false);
            int upsertBatchSize = conn.getMutateBatchSize();
            String upsertStatement = "upsert into youku values(?,?,?,?)";
            stmt = conn.prepareStatement(upsertStatement);
            int rowCount = 0;
            for (int i = 0; i < 100000000; i++) {
                Random r = new Random();
                int d = r.nextInt(1000);
                String id = "id" + i;
                String name = "name" + d;
                int click = r.nextInt(100);
                double time = r.nextDouble() * 100;
                stmt.setString(1, id);
                stmt.setString(2, name);
                stmt.setInt(3, click);
                stmt.setDouble(4, time);
                stmt.execute();
// Commit when batch size is reached
                if (++ rowCount  % upsertBatchSize == 0) {
                    conn.commit();
                    System.out.println("Rows upserted: " + rowCount);
                }
            }
            conn.commit();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}