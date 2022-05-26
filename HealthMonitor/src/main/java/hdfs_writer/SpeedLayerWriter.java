package hdfs_writer;

import message.HealthMessage;

import java.io.*;

public class SpeedLayerWriter {
    private int fileCounter = 0;
    private static String inputPath = "/home/h-user/IntelliJProjects/Spark/src/main/resources/sparkInput";

    public void writeMessages(HealthMessage[] messages){
        File file = new File(inputPath + "/" + fileCounter++);
        try {
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for(HealthMessage message : messages){
                bw.write(message.toString());
                bw.newLine();
            }
            bw.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
