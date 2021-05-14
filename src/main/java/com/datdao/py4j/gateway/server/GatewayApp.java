package com.datdao.py4j.gateway.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import py4j.GatewayServer;

/**
 * @author ddao
 */
public class GatewayApp {

    private Connection connection;
    private String url;
    private String user;
    private String password;

    public static void main(String[] args) {
        GatewayApp app = new GatewayApp();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        System.out.println("Starting py4j gateway");
        server.start();
    }

    public void setConnectionInfo(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public void openDbConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                return;
            }
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, user, password);
            System.out.println(String.format("Connected to [%s] with schema [%s]", url, user));
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public int executeUpdate(String sql) {
        try (Statement stmt = connection.createStatement()) {
            System.out.println(String.format("Executing query [%s]", sql));
            return stmt.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println(e);
        }
        return 0;
    }

    public List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println(String.format("Executing query [%s]", sql));
            ResultSetMetaData rsMetaData = rs.getMetaData();

            while (rs.next()) {
                Map<String, Object> record = new HashMap<>(rsMetaData.getColumnCount());
                for (int idx = 1; idx <= rsMetaData.getColumnCount(); idx++) {
                    String columnName = rsMetaData.getColumnName(idx);
                    record.put(columnName, rs.getObject(columnName));
                }
                result.add(record);
            }
            System.out.println("Fetched " + result.size() + " records");
        } catch (Exception e) {
            System.out.println(e);
        }
        return result;
    }

    public void closeDbConnection() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
                System.out.println(String.format("Closed database connection [%s] with schema [%s]", url, user));
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
