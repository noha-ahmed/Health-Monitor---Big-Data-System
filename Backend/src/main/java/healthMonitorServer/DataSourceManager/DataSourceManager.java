package healthMonitorServer.DataSourceManager;

import healthMonitorServer.dto.RecordDTO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;

public class DataSourceManager {
   private static DataSourceManager manager = new DataSourceManager();
   private String realTimeViewsPath;
   private String batchViewsPath;

   private DataSourceManager(){}

   public static DataSourceManager getInstance(){
       return manager;
   }
   public void initialize(String realTimeViewsPath, String batchViewsPath){
       Connection conn = initializeConnection();
       this.realTimeViewsPath = realTimeViewsPath;
       RealTimeViewsManager.getInstance().initiallize(realTimeViewsPath, conn);
//       BatchViewsManager.getInstance().initiallize(batchViewsPath, conn);
   }

   public ArrayList<RecordDTO> getRecords(long from, long to)  {
       ArrayList<RecordDTO> records = new ArrayList<>();
       ResultSet realTimeResult =  RealTimeViewsManager.getInstance().getRecords(from, to);
//       ResultSet batchResult =  BatchViewsManager.getInstance().getRecords(from, to);
       try{
           while (realTimeResult.next()){
               String servName = realTimeResult.getString("serviceName");
               int count = realTimeResult.getInt("MSG_COUNT");
               float cpu_mean = realTimeResult.getFloat("CPU_SUM")/count;
               float ram_mean = realTimeResult.getFloat("RAM_SUM")/count;
               float disk_mean = realTimeResult.getFloat("Disk_SUM")/count;
               long cpu_peek = realTimeResult.getLong("CPU_Peek_T");
               long ram_peek = realTimeResult.getLong("RAM_Peek_T");
               long disk_peek = realTimeResult.getLong("Disk_Peek_T");
//               int count = batchResult.getInt("MSG_COUNT")+realTimeResult.getInt("MSG_COUNT");
//               float cpu_mean = (batchResult.getFloat("CPU_SUM")+realTimeResult.getFloat("CPU_SUM"))/count;
//               float ram_mean = (batchResult.getFloat("RAM_SUM")+realTimeResult.getFloat("RAM_SUM"))/count;
//               float disk_mean = (batchResult.getFloat("Disk_SUM")+realTimeResult.getFloat("Disk_SUM"))/count;
//               long cpu_peek;
//               if(batchResult.getFloat("CPU_MAX")>=realTimeResult.getFloat("CPU_MAX")){
//                   cpu_peek = batchResult.getLong("CPU_Peek_T");
//               }else{
//                   cpu_peek = realTimeResult.getLong("CPU_Peek_T");
//               }
//               long ram_peek;
//               if(batchResult.getFloat("RAM_MAX")>=realTimeResult.getFloat("RAM_MAX")){
//                   ram_peek = batchResult.getLong("RAM_Peek_T");
//               }else{
//                   ram_peek = realTimeResult.getLong("RAM_Peek_T");
//               }
//               long disk_peek;
//               if(batchResult.getFloat("Disk_MAX")>=realTimeResult.getFloat("Disk_MAX")){
//                   disk_peek = batchResult.getLong("Disk_Peek_T");
//               }else{
//                   disk_peek = realTimeResult.getLong("Disk_Peek_T");
//               }
//               rec = RecordDTO.builder().
//                       serviceName(realTimeResult.getNString("serviceName"))
//                       .meanCpuUtilization(String.valueOf(cpu_mean)).peakTimeCpu(String.valueOf(cpu_peek))
//                       .meanRamUtilization(String.valueOf(ram_mean)).peakTimeRam(String.valueOf(ram_peek))
//                       .meanDiskUtilization(String.valueOf(disk_mean)).peakTimeDisk(String.valueOf(disk_peek))
//                       .messagesCount(String.valueOf(count))
//                       .build();

                RecordDTO rec = RecordDTO.builder().
                       serviceName(servName)
                       .meanCpuUtilization(String.valueOf(cpu_mean)).peakTimeCpu(String.valueOf(cpu_peek))
                       .meanRamUtilization(String.valueOf(ram_mean)).peakTimeRam(String.valueOf(ram_peek))
                       .meanDiskUtilization(String.valueOf(disk_mean)).peakTimeDisk(String.valueOf(disk_peek))
                       .messagesCount(String.valueOf(count))
                       .build();
               records.add(rec);

           }

       }catch (Exception e){

       }
       System.out.println(records.size());

       return records;
   }

   private Connection initializeConnection(){
       Connection conn = null;
       try {
           Class.forName("org.duckdb.DuckDBDriver");
           conn = DriverManager.getConnection("jdbc:duckdb:");
       } catch (Exception e) {
           throw new RuntimeException(e);
       }
       return conn;
   }

   public static void main(String[] args){
       DataSourceManager.getInstance().initialize("/home/h-user/IntelliJProjects/Spark/src/main/resources/sparkOutput/*.parquet","");
       DataSourceManager.getInstance().getRecords(0L,1647958696);
   }
}
