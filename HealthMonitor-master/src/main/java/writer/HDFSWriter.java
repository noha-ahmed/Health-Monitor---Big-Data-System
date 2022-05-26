package writer;

import com.opencsv.CSVWriter;
import message.HealthMessage;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class HDFSWriter {

    DateFormat dateFormat = new SimpleDateFormat("dd-mm-yyyy");
    FileSystem fileSystem;
    CSVWriter writer;
    BufferedWriter br;
    String currentPath = "";

//    Path filePath;

    private double[] writtenTime = new double[1024];


    public HDFSWriter(){

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.set("dfs.replication", "1");
        try {
            fileSystem = FileSystem.get(configuration);
            String directoryName = "/bigdata_dataset";
//            if(! new File("bigdata_dataset").exists()){
//                new File("bigdata_dataset").mkdirs();
//            }
            Path path = new Path(directoryName);
            if(!fileSystem.exists(path))
                fileSystem.mkdirs(path);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public Path createFile(LocalDateTime date){
//        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd_mm_yyyy");
//        LocalDateTime now = LocalDateTime.now();
//        Path filePath = null;
//        try {
//            String file = dtf.format(now) + ".log";
//            filePath = new Path(file);
//            stream = fileSystem.create(filePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return filePath;
//    }

    public CSVWriter createCSVFile(String filePath){
        Path path = new Path(filePath);
        FSDataOutputStream stream;
//        File file = new File(filePath);
//        try {
//            // create FileWriter object with file as parameter
//            FileWriter outputfile = new FileWriter(file, true);
//
//            // create CSVWriter object filewriter object as parameter
//            writer = new CSVWriter(outputfile);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        try {
            stream = fileSystem.create(path);
            br = new BufferedWriter( new OutputStreamWriter( stream, StandardCharsets.UTF_8 ) );
            if(!(writer == null))
                writer.close();
            writer = new CSVWriter(br);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer;
    }

//    public void writeMessageToHDFS(String[] messages){
//        try {
//            if(fileSystem.exists(filePath)){
//                createFile();
//            }else {
//                stream = fileSystem.append(filePath);
//            }
//            double start = System.currentTimeMillis();
//            for(int i = 0; i < 1024; i++){
//                stream.write(messages[i].getBytes());
//                // msg is written
//                writtenTime[i] = System.currentTimeMillis();
//            }
//            double end = System.currentTimeMillis();
//
//            // calculating time taken to write data in hdfs
//            System.out.println("Time taken to write data: " + (end - start));
//            // average end to end time from receiving to writing
//            double avg = 0;
//            for(int i = 0; i < 1024; i++){
//                avg += (writtenTime[i] - HealthMessagesReceiver.arrivalTime[i])  ;
//                avg /= 1024;
//            }
//            System.out.println("Average end to end time from receiving to writing: " + avg);
//            System.out.println("Throughput: " + (1024 / avg));
//            stream.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public void closeWriter(){
        if(writer == null)
            return;
        try {
            writer.close();
            writer = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeMessageToCSV(HealthMessage message){
        // serviceName Timestamp CPU RAM(Free total) Disk(Free total)
        writer.writeNext(message.csvData());
    }

    public String getPath(String timestamp){
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new java.util.Date(Long.parseLong(timestamp)*1000));
        String toGetFolder = date.split(" ")[0];
        return toGetFolder;
    }
//    public void writeMessageToHDFS(HealthMessage[] messages){
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//        configuration.set("dfs.replication", "1");
//        try {
//            fileSystem = FileSystem.get(configuration);
//            for(int i = 0; i < messages.length; i++){
//                HealthMessage message = messages[i];
//                String path = "bigdata_dataset/" + getPath(message.getTimestamp() + "") + ".csv";
//                if(!currentPath.equals(path)){
//                    currentPath = path;
//                    closeWriter();
//                    writer = createCSVFile(path);
//                }
//                writeMessageToCSV(message);
//            }
//            br.close();
//            fileSystem.close();;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
    public void writeMessageToHDFS(HealthMessage[] messages){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.set("dfs.replication", "1");
        try {
            fileSystem = FileSystem.get(configuration);
            FSDataOutputStream stream ;
            String path = "/bigdata_dataset/" + getPath(messages[0].getTimestamp() + "") + ".csv";
            if(!currentPath.equals(path)){
                stream = fileSystem.create(new Path(path));
            }else{
                stream = fileSystem.append(new Path(path));
            }
            BufferedWriter bufferedWriter = new BufferedWriter( new OutputStreamWriter( stream, StandardCharsets.UTF_8 ) );
            CSVWriter csvWriter = new CSVWriter(bufferedWriter);
            for(int i = 0; i < messages.length; i++){
                HealthMessage message = messages[i];
                path = "/bigdata_dataset/" + getPath(message.getTimestamp() + "") + ".csv";
                if(!currentPath.equals(path)){
                    currentPath = path;
                    stream = fileSystem.create(new Path(path));
                    bufferedWriter = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
//                    closeWriter();
                    csvWriter.close();
                    csvWriter = new CSVWriter(bufferedWriter);
                }
                writeMessageToCSV(message);
            }
            bufferedWriter.close();
            fileSystem.close();;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
