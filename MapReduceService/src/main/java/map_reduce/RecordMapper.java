package map_reduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;


public class RecordMapper extends Mapper<Object,Text, Text, RecordWritable> {
    public static long recordsCount = 0;
    private final static IntWritable one = new IntWritable(1);
    private Text RecordKey = new Text();
    private RecordWritable recordWritable;


//    @Override
//    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
//        // get the searchingWord from configuration
//
//    }

    private String getDate(String timestamp){
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String date = sdf.format(new java.util.Date(Long.parseLong(timestamp)*1000));
        date = date + ":00";
        return date;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        System.out.printf("mapper value  " + value.toString());
        String[] message = value.toString().split(",");

        String date = getDate(message[1]);
        recordWritable = new RecordWritable();
        RecordKey.set(message[0] + " " + date); // service name + date
        recordWritable.setTimestamp(new LongWritable(Long.parseLong(message[1])));
        recordWritable.setServiceName(new Text(message[0]));
        recordWritable.setTotalCPU(new FloatWritable(Float.parseFloat(message[2])));
        recordWritable.setMaxCPU(new FloatWritable(Float.parseFloat(message[2])));
        recordWritable.setTotalRAM(new FloatWritable(Float.parseFloat(message[4]) - Integer.parseInt(message[3])));
        recordWritable.setMaxRAM(new FloatWritable(Float.parseFloat(message[4]) - Integer.parseInt(message[3])));
        recordWritable.setTotalDisk(new FloatWritable(Float.parseFloat(message[6]) - Integer.parseInt(message[5])));
        recordWritable.setMaxDISK(new FloatWritable(Float.parseFloat(message[6]) - Integer.parseInt(message[5])));
        recordWritable.setPeekTimeDisk(new LongWritable(Long.parseLong(message[1])));
        recordWritable.setCount(one);
        try {
            context.write(RecordKey, recordWritable);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}


