package map_reduce;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements Writable {
    private Text serviceName;
    private LongWritable timestamp;
    private FloatWritable totalCPU;
    private LongWritable peekTimeCPU;
    private FloatWritable maxCPU;
    private FloatWritable totalRAM;
    private LongWritable peekTimeRam;
    private FloatWritable maxRAM;
    private FloatWritable totalDisk;
    private LongWritable peekTimeDisk;
    private FloatWritable maxDISK;
    private IntWritable count;
    //default constructor for (de)serialization
    public RecordWritable() {
        serviceName = new Text();
        totalCPU = new FloatWritable(0);
        peekTimeCPU = new LongWritable(0);
        maxCPU = new FloatWritable(0);
        totalRAM = new FloatWritable(0);
        peekTimeRam = new LongWritable(0);
        maxRAM = new FloatWritable(0);
        totalDisk = new FloatWritable(0);
        peekTimeDisk = new LongWritable(0);
        maxDISK = new FloatWritable(0);
        timestamp = new LongWritable(0);
        count = new IntWritable(0);
    }
    public void write(DataOutput dataOutput) throws IOException {
        serviceName.write(dataOutput); //write serviceId
        totalCPU.write(dataOutput); //write totalCPU
        peekTimeCPU.write(dataOutput); //write peekTimeCPU
        maxCPU.write(dataOutput);
        totalRAM.write(dataOutput); //write totalRAM
        peekTimeRam.write(dataOutput);//write peekTimeRAM
        maxRAM.write(dataOutput);
        totalDisk.write(dataOutput); //write totalDisk
        peekTimeDisk.write(dataOutput); //write peekTimeDisk
        maxDISK.write(dataOutput);
        timestamp.write(dataOutput); //write timestamp
        count.write(dataOutput); //write count
    }
    public void readFields(DataInput dataInput) throws IOException {
        serviceName.readFields(dataInput); //read serviceId
        totalCPU.readFields(dataInput); //read totalCPU
        peekTimeCPU.readFields(dataInput); //read peekTimeCPU
        maxCPU.readFields(dataInput);
        totalRAM.readFields(dataInput); //read totalRAM
        peekTimeRam.readFields(dataInput); //read peekTimeRam
        maxRAM.readFields(dataInput);
        totalDisk.readFields(dataInput); //read totalDisk
        peekTimeDisk.readFields(dataInput); //read peekTimeDisk
        maxDISK.readFields(dataInput);
        timestamp.readFields(dataInput); //read timestamp
        count.readFields(dataInput); //read count
    }

    public Text getServiceName() {
        return serviceName;
    }

    public void setServiceName(Text serviceName) {
        this.serviceName = serviceName;
    }

    public FloatWritable getTotalCPU() {
        return totalCPU;
    }

    public void setTotalCPU(FloatWritable totalCPU) {
        this.totalCPU = totalCPU;
    }

    public FloatWritable getTotalRAM() {
        return totalRAM;
    }

    public void setTotalRAM(FloatWritable totalRAM) {
        this.totalRAM = totalRAM;
    }

    public FloatWritable getTotalDisk() {
        return totalDisk;
    }

    public void setTotalDisk(FloatWritable totalDisk) {
        this.totalDisk = totalDisk;
    }

    public LongWritable getPeekTimeCPU() {
        return peekTimeCPU;
    }

    public void setPeekTimeCPU(LongWritable peekTimeCPU) {
        this.peekTimeCPU = peekTimeCPU;
    }

    public LongWritable getPeekTimeRam() {
        return peekTimeRam;
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LongWritable timestamp) {
        this.timestamp = timestamp;
    }

    public void setPeekTimeRam(LongWritable peekTimeRam) {
        this.peekTimeRam = peekTimeRam;
    }

    public LongWritable getPeekTimeDisk() {
        return peekTimeDisk;
    }

    public void setPeekTimeDisk(LongWritable peekTimeDisk) {
        this.peekTimeDisk = peekTimeDisk;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public FloatWritable getMaxCPU() {
        return maxCPU;
    }

    public void setMaxCPU(FloatWritable maxCPU) {
        this.maxCPU = maxCPU;
    }

    public FloatWritable getMaxRAM() {
        return maxRAM;
    }

    public void setMaxRAM(FloatWritable maxRAM) {
        this.maxRAM = maxRAM;
    }

    public FloatWritable getMaxDISK() {
        return maxDISK;
    }

    public void setMaxDISK(FloatWritable maxDISK) {
        this.maxDISK = maxDISK;
    }

    public RecordWritable(Text serviceName, LongWritable timestamp,
                          FloatWritable totalCPU, LongWritable peekTimeCPU, FloatWritable maxCPU,
                          FloatWritable totalRAM, LongWritable peekTimeRam, FloatWritable maxRAM,
                          FloatWritable totalDisk, LongWritable peekTimeDisk, FloatWritable maxDISK,
                          IntWritable count) {
        this.serviceName = serviceName;
        this.timestamp = timestamp;
        this.totalCPU = totalCPU;
        this.peekTimeCPU = peekTimeCPU;
        this.maxCPU = maxCPU;
        this.totalRAM = totalRAM;
        this.peekTimeRam = peekTimeRam;
        this.maxRAM = maxRAM;
        this.totalDisk = totalDisk;
        this.peekTimeDisk = peekTimeDisk;
        this.maxDISK = maxDISK;
        this.count = count;
    }

    public void addTotalCPU(FloatWritable totalAge) {
        this.totalCPU.set(this.totalCPU.get()+totalAge.get());
    }

    public void addTotalRAM(FloatWritable totalRAM) {
        this.totalRAM.set(this.totalRAM.get()+totalRAM.get());
    }

    public void addTotalDisk(FloatWritable totalDisk) {
        this.totalDisk.set(this.totalDisk.get()+totalDisk.get());
    }

    public void addTotalCount(IntWritable count) {
        this.count.set(this.count.get()+count.get());
    }

    @Override
    public String toString() {
        return serviceName +
                ", " + timestamp +
                ", " + totalCPU +
                ", " + peekTimeCPU +
                ", " + maxCPU +
                ", " + totalRAM +
                ", " + peekTimeRam +
                ", " + maxRAM +
                ", " + totalDisk +
                ", " + peekTimeDisk +
                ", " + maxDISK +
                ", " + count;
    }
}
