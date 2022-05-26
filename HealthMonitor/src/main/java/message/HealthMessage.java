package message;

import java.io.Serializable;

public class HealthMessage {
    private class RAM implements Serializable{
        private int Total;
        private float Free;

        public RAM(){}

        public RAM(int total, float free) {
            Total = total;
            Free = free;
        }

        public int getTotal() {
            return Total;
        }

        public void setTotal(int total) {
            Total = total;
        }

        public float getFree() {
            return Free;
        }

        public void setFree(float free) {
            Free = free;
        }

        @Override
        public String toString() {
            return "RAM{" +
                    "Total=" + Total +
                    ", Free=" + Free +
                    '}';
        }
    }

    private class Disk implements Serializable {
        private int Total;
        private float Free;

        public Disk(){}

        public Disk(int total, float free) {
            Total = total;
            Free = free;
        }

        public int getTotal() {
            return Total;
        }

        public void setTotal(int total) {
            Total = total;
        }

        public float getFree() {
            return Free;
        }

        public void setFree(float free) {
            Free = free;
        }

        @Override
        public String toString() {
            return "Disk{" +
                    "Total=" + Total +
                    ", Free=" + Free +
                    '}';
        }
    }

    private String serviceName;
    private long Timestamp;
    private float CPU;
    private RAM RAM;
    private Disk Disk;

    public HealthMessage(){}

    public HealthMessage(String serviceName, long timestamp, float CPU, HealthMessage.RAM RAM, HealthMessage.Disk disk) {
        this.serviceName = serviceName;
        Timestamp = timestamp;
        this.CPU = CPU;
        this.RAM = RAM;
        Disk = disk;
    }


    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(long timestamp) {
        Timestamp = timestamp;
    }

    public float getCPU() {
        return CPU;
    }

    public void setCPU(float CPU) {
        this.CPU = CPU;
    }

    public HealthMessage.RAM getRAM() {
        return RAM;
    }

    public void setRAM(HealthMessage.RAM RAM) {
        this.RAM = RAM;
    }

    public HealthMessage.Disk getDisk() {
        return Disk;
    }

    public void setDisk(HealthMessage.Disk disk) {
        Disk = disk;
    }

    @Override
    public String toString() {
        return "healthMessage{" +
                "serviceName='" + serviceName + '\'' +
                ", Timestamp=" + Timestamp +
                ", CPU=" + CPU +
                ", RAM=" + RAM +
                ", Disk=" + Disk +
                '}';
    }

    public String[] csvData(){
        String[] data = new String[7];
        data[0] = serviceName;
        data[1] = Timestamp + "";
        data[2] = CPU + "";
        data[3] = RAM.Free + "";
        data[4] = RAM.Total + "";
        data[5] = Disk.Free + "";
        data[6] = Disk.Total + "";
        return data;
    }
}