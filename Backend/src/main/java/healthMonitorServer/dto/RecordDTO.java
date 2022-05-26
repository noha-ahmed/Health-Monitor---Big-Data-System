package healthMonitorServer.dto;

import lombok.*;

@Setter
@Getter
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecordDTO {
    String serviceName;
    String meanCpuUtilization;
    String peakTimeCpu;
    String meanRamUtilization;
    String peakTimeRam;
    String meanDiskUtilization;
    String peakTimeDisk;
    String messagesCount;
}
