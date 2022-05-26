package SpeedLayer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.*;

public class RealTimeViewsManager {
    private static String viewsPath;
    private static Connection conn;
    private static PreparedStatement p_stmt;
    private static PreparedStatement t_stmt;
    private static Statement stmt;
    public static void initiallize()  {
        if( viewsPath == null ){
            System.out.println("Views Path is not set.");
            return;
        }
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            conn = DriverManager.getConnection("jdbc:duckdb:");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setViewsPath(String path){
        viewsPath = path;
    }

    public static void getRecords(long from, long to) {
        System.out.println(new Timestamp(1647938696L *1000));
        try {
            p_stmt = conn.prepareStatement("CREATE OR REPLACE TEMP TABLE t1 AS SELECT * FROM '"+ viewsPath + "' " +
                "WHERE Timestamp BETWEEN ? AND ?; ");
            p_stmt.setLong(1,from);
            p_stmt.setLong(2,to);
            p_stmt.execute();
//            p_stmt = conn.prepareStatement("SELECT * FROM t1");
            p_stmt = conn.prepareStatement("SELECT DISTINCT T1.serviceName, T2.Timestamp AS CPU_Peek_T, " +
                    "T3.Timestamp AS RAM_Peek_T, T4.Timestamp AS Disk_Peek_T, T1.CPU_MAX," +
                    "T1.RAM_MAX, T1.Disk_MAX, T1.MSG_COUNT, T1.CPU_SUM,T1.RAM_SUM, Disk_SUM  FROM " +
                    "((((SELECT serviceName," +
                    "COUNT(*) AS MSG_COUNT, " +
                    "SUM(CPU) AS CPU_SUM, MAX(CPU) AS CPU_MAX,  " +
                    "SUM(RAM) AS RAM_SUM, MAX(RAM) AS RAM_MAX, " +
                    "SUM(Disk) AS Disk_SUM, MAX(Disk) AS Disk_MAX, " +
                    "FROM t1 " +
                    "GROUP BY serviceName) AS T1 " +
                    "JOIN " +
                    "(SELECT serviceName, Timestamp, CPU " +
                    "FROM t1 ) AS T2 " +
                    "ON (T1.serviceName=T2.serviceName AND T1.CPU_MAX=T2.CPU) )" +
                    "JOIN " +
                    "(SELECT serviceName, Timestamp, RAM " +
                    "FROM t1 ) AS T3 " +
                    "ON (T1.serviceName=T3.serviceName AND T1.RAM_MAX = T3.RAM) ) " +
                    "JOIN " +
                    "(SELECT serviceName, Timestamp, Disk " +
                    "FROM t1 ) AS T4 " +
                    "ON (T1.serviceName=T4.serviceName AND T1.Disk_MAX = T4.Disk)) " +
                    "ORDER BY T1.serviceName");

            ResultSet rs = p_stmt.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            System.out.println(md);
            int row = 1;
            for (int col = 1; col <= md.getColumnCount(); col++) {
                System.out.print(md.getColumnName(col) + "  ");
            }
            System.out.println();
            while (rs.next()) {
                for (int col = 1; col <= md.getColumnCount(); col++) {
                    System.out.print( rs.getString(col) + " ");
                }
                System.out.println();
                row++;
            }

//            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args){
        RealTimeViewsManager.setViewsPath("src/main/resources/sparkOutput/*.parquet");
        RealTimeViewsManager.initiallize();
        RealTimeViewsManager.getRecords(1647938500,1647938700);
    }
}
