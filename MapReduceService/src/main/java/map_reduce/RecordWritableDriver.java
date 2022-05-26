package map_reduce;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.quartz.*;

import java.io.IOException;

public class RecordWritableDriver extends Configured{

//    public RecordWritableDriver(){
//        String[] args2 = {"/bigdata_dataset/2025-03-21.csv",
//                "/health_data_output"};
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//        try {
//            FileSystem fileSystem = FileSystem.get(configuration);
//            String directoryName = args2[1];
//            Path path = new Path(directoryName);
//            if( fileSystem.exists(path))
//                fileSystem.delete(path,true);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) throws Exception {
//        String[] args2 = {"/health_data2/health_0.json",
//               "/health_data_output2"};
        System.setProperty("hadoop.home.dir", "/");
        String[] args2 = {"/user/h-user/bigdata_dataset",
                "/health_data_output2"};
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            String directoryName = args2[1];
            Path path = new Path(directoryName);
            if( fileSystem.exists(path))
                fileSystem.delete(path,true);

        } catch (IOException e) {
            e.printStackTrace();
        }

        int exitCode = run(args2, 1648440097, System.currentTimeMillis());

//        try {
//
//            // specify the job' s details..
//            JobDetail scheduledJob = JobBuilder.newJob(ScheduleJob.class)
//                    .withIdentity("testJob")
//                    .build();
//
//            // specify the running period of the job
//            Trigger trigger = TriggerBuilder.newTrigger()
//                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
//                            .withIntervalInSeconds(30)
//                            .repeatForever())
//                    .build();
//
//            //schedule the job
//            SchedulerFactory schFactory = new StdSchedulerFactory();
//            Scheduler sch = schFactory.getScheduler();
//            sch.start();
//            sch.scheduleJob(scheduledJob, trigger);
//
//        } catch (SchedulerException e) {
//            e.printStackTrace();
//        }

//        System.out.println("All records : " + RecordMapper.recordsCount);
        System.exit(exitCode);
    }

    private static Schema parseSchema() {
        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"serviceName\",  \"type\": \"string\"}"
                + ", {\"name\": \"timestamp\", \"type\": \"long\"}"
                + ", {\"name\": \"totalCPU\",  \"type\": \"float\"}"
                + ", {\"name\": \"PeekTimeCPU\", \"type\": \"long\"}"
                + ", {\"name\": \"maxCPU\", \"type\": \"float\"}"
                + ", {\"name\": \"totalRAM\",  \"type\": \"float\"}"
                + ", {\"name\": \"PeekTimeRAM\", \"type\": \"long\"}"
                + ", {\"name\": \"maxRAM\", \"type\": \"float\"}"
                + ", {\"name\": \"totalDisk\",  \"type\": \"float\"}"
                + ", {\"name\": \"PeekTimeDISK\", \"type\": \"long\"}"
                + ", {\"name\": \"maxDISK\", \"type\": \"float\"}"
                + ", {\"name\": \"count\", \"type\": \"int\"}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    public static int run(String[] args, long from, long to) throws Exception {
        if (args.length != 2) {
            System.out.println("Please provide two arguments :");
            System.out.println("[ 1 ] Input dir path");
            System.out.println("[ 2 ] Output dir path");
            return -1;
        }
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

//        conf.set("fromDate", from + "");
//        conf.set("toDate", to + "");

//        conf.set("mapreduce.job.maps","5");
//        conf.set("mapreduce.job.reduces","3");
        Job job=Job.getInstance(conf,"Hadoop Health Message Writer");
        job.setJarByClass(RecordWritableDriver.class);
        job.setMapperClass(RecordMapper.class);
        job.setReducerClass(RecordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecordWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RecordWritable.class);
        job.setNumReduceTasks(4);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // setting schema
        Schema MAPPING_SCHEMA = parseSchema();
        AvroParquetOutputFormat.setSchema(job, MAPPING_SCHEMA);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean success = job.waitForCompletion(true);




        return (success?0:1);


    }


}
