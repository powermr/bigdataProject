import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveDemo {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.217.129:10000/test";
    private static String user = "root";
    private static String password = "hadoop";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    @Before
    public void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        stmt = conn.createStatement();
    }

    @Test
    public void createDatabases() throws SQLException {
        String sql = "create database hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);

    }

    @Test
    public void showDatabases() throws SQLException {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @After
    public void destory() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
