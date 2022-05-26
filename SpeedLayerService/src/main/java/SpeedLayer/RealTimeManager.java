package SpeedLayer;


import java.util.Scanner;

public class RealTimeManager {
    public static void main (String[] args){
        String inputPath = "src/main/resources/sparkInput";
        String outputPath = "src/main/resources/sparkOutput";
        SparkStream sp = new SparkStream(inputPath, outputPath);
        sp.start();
        Scanner sc = new Scanner(System.in);
//        while(true){
//            String s = sc.next();
//            if( s.equals("stop")){
//                System.out.println("hereee");
////                sp = new SparkStream(inputPath + "2" , outputPath +"2");
//                sp.spark.stop();
//            }
////            DataSourceManager.getInstance().getRecords(0L,1647958696);
////
////            RealTimeViewsManager.getRecords(0L,1647958696);
//
//        }


    }


}
