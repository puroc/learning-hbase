package com.example.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestPhoenixSelect {

    public static void main(String[] args) {
        PhoenixConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:cdh0");
            conn.setAutoCommit(false);

//            int upsertBatchSize = conn.getMutateBatchSize();
            String upsertStatement = "select * from PUD_TEST limit 10";
            stmt = conn.prepareStatement(upsertStatement);
            stmt.execute();
            conn.commit();
            rs =  stmt.getResultSet();
            while(rs.next()){
                System.out.println(rs.getInt("READING"));
                System.out.println(rs.getInt("REALVALUE"));
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }  finally {
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