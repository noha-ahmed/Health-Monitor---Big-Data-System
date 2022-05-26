package writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class main {

    static FileSystem fileSystem;
    static FSDataOutputStream stream;

    public static Path createFile(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd_mm_yyyy");
        LocalDateTime now = LocalDateTime.now();
        Path filePath = null;
        try {
            String file = dtf.format(now) + ".log";
            System.out.println(file);
            filePath = new Path(file);
            stream = fileSystem.create(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filePath;
    }

    public static void main(String[] args) throws ParseException {
//        String timestamp = "1647938696";
//        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String date = sdf.format(new Date(Long.parseLong(timestamp)*1000));
//        System.out.println(date);
//        String toGetFolder = date.split(" ")[0];
//        System.out.println(toGetFolder);
//        String folder = String.valueOf((sdf.parse(toGetFolder+" 00:00:00").getTime()) /1000);
//        System.out.println(folder);
        String timestamp = "1647938696";
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new java.util.Date(Long.parseLong(timestamp)*1000));
        System.out.println(date);
    }
    public void get(){}

}
