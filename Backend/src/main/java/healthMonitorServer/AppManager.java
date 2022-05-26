package healthMonitorServer;

import healthMonitorServer.DataSourceManager.DataSourceManager;
import healthMonitorServer.dto.RecordDTO;

import java.util.ArrayList;

public class AppManager {
    private static String realTimeViewsPath = "/home/h-user/IntelliJProjects/Spark/src/main/resources/sparkOutput/*.parquet";
    private static String batchViewsPath = " ";
    public static ArrayList<RecordDTO> getRecords(long from, long to){
        DataSourceManager.getInstance().initialize(realTimeViewsPath,batchViewsPath);
        return DataSourceManager.getInstance().getRecords(from,to);
    }
}
