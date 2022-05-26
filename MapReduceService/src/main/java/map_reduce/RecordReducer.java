package map_reduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecordReducer extends Reducer<Text, RecordWritable,Text,RecordWritable> {
    @Override
    protected void reduce(Text key, Iterable<RecordWritable> values, Context context) throws IOException, InterruptedException {
        RecordWritable aggregateServiceAnalysis = new RecordWritable();
        int count = 0;
        long peekTimeCPU = 0;
        long peekTimeRAM = 0;
        long peekTimeDisk = 0;
        float highestCPU = 0;
        float highestRAM = 0;
        float highestDisk = 0;
        for(RecordWritable familyWritable : values){
            if(familyWritable.getTotalCPU().get() > highestCPU)
                peekTimeCPU = familyWritable.getPeekTimeDisk().get();

            if(familyWritable.getTotalRAM().get() > highestRAM)
                peekTimeRAM = familyWritable.getPeekTimeDisk().get();

            if(familyWritable.getTotalDisk().get() > highestDisk)
                peekTimeDisk = familyWritable.getPeekTimeDisk().get();

            aggregateServiceAnalysis.addTotalCPU(familyWritable.getTotalCPU());
            aggregateServiceAnalysis.addTotalRAM(familyWritable.getTotalRAM());
            aggregateServiceAnalysis.addTotalDisk(familyWritable.getTotalDisk());
            aggregateServiceAnalysis.setTimestamp(familyWritable.getTimestamp());
            count++;
        }
        aggregateServiceAnalysis.setPeekTimeCPU(new LongWritable(peekTimeCPU));
        aggregateServiceAnalysis.setMaxCPU(new FloatWritable(highestCPU));
        aggregateServiceAnalysis.setPeekTimeRam(new LongWritable(peekTimeRAM));
        aggregateServiceAnalysis.setMaxRAM(new FloatWritable(highestRAM));
        aggregateServiceAnalysis.setPeekTimeDisk(new LongWritable(peekTimeDisk));
        aggregateServiceAnalysis.setMaxDISK(new FloatWritable(highestDisk));
        aggregateServiceAnalysis.addTotalCount(new IntWritable(count));
        String[] keyElements = key.toString().split(" ");
        aggregateServiceAnalysis.setServiceName(new Text(keyElements[0]));
        context.write(new Text(keyElements[0]),aggregateServiceAnalysis);
    }
}


