package healthMonitorServer.DataSourceManager;

import java.sql.*;

public class BatchViewsManager {
    private String viewsPath;
    private Connection conn;
    private PreparedStatement p_stmt;
    private static BatchViewsManager manager = new BatchViewsManager();

    private BatchViewsManager(){}

    public static BatchViewsManager getInstance(){
        return manager;
    }

    public void initiallize(String path, Connection conn)  {
       this.viewsPath = path;
       this.conn = conn;
    }

    public void setViewsPath(String path){
        viewsPath = path;
    }

    public  void formStatement(long from, long to){
        try {
            p_stmt = conn.prepareStatement("CREATE OR REPLACE TEMP TABLE t1 AS SELECT * FROM '" + viewsPath + "' " +
                    "WHERE Timestamp BETWEEN ? AND ?; ");
            p_stmt.setLong(1, from);
            p_stmt.setLong(2, to);
            p_stmt.execute();
//            p_stmt = conn.prepareStatement("SELECT * FROM t1");
            p_stmt = conn.prepareStatement("SELECT T1.ServiceName, T2.Timestamp AS CPU_Peek_T, " +
                    "T3.Timestamp AS RAM_Peek_T, T4.Timestamp AS Disk_Peek_T, T1.CPU_MAX," +
                    "T1.RAM_MAX, T1.Disk_MAX, T1.MSG_COUNT, T1.CPU_SUM,T1.RAM_SUM, Disk_SUM  FROM " +
                    "((((SELECT ServiceName, " +
                    "SUM(Count) AS MSG_COUNT, " +
                    "SUM(TotalCPU) AS CPU_SUM, MAX(MaxCPU ) AS CPU_MAX,  " +
                    "SUM(TotalRAM) AS RAM_SUM, MAX(MaxRAM) AS RAM_MAX, " +
                    "SUM(TotalDISK) AS Disk_SUM, MAX(MaxDISK) AS Disk_MAX, " +
                    "FROM t1 " +
                    "GROUP BY ServiceName) AS T1 " +
                    "JOIN " +
                    "(SELECT ServiceName, Timestamp, MaxCPU " +
                    "FROM t1 ) AS T2 " +
                    "ON (T1.ServiceName=T2.ServiceName AND T1.CPU_MAX=T2.MaxCPU) )" +
                    "JOIN " +
                    "(SELECT ServiceName, Timestamp, MaxRAM " +
                    "FROM t1 ) AS T3 " +
                    "ON (T1.ServiceName=T3.ServiceName AND T1.RAM_MAX = T3.MaxRAM) ) " +
                    "JOIN " +
                    "(SELECT ServiceName, Timestamp, MaxDISK " +
                    "FROM t1 ) AS T4 " +
                    "ON (T1.ServiceName=T4.ServiceName AND T1.Disk_MAX = T4.MaxDISK)) " +
                    "ORDER BY T1.ServiceName");
        }
        catch (SQLException e) {

        }

    }

    public ResultSet getRecords(long from, long to) {
        ResultSet rs = null;
        try {
            formStatement(from, to);
            rs = p_stmt.executeQuery();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return rs;
    }

    public void printResult(ResultSet rs){
        try {
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args){
        //**/RealTimeViewsManager.setViewsPath("src/main/resources/sparkOutput/*.parquet");
//        RealTimeViewsManager.getInstance().initiallize("src/main/resources/sparkOutput/*.parquet");
//        RealTimeViewsManager.getInstance().getRecords(1647938500,1647938700);
    }
}
