package healthMonitorServer.controller;

import healthMonitorServer.AppManager;
import healthMonitorServer.dto.RecordDTO;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/api")
public class Controller {

    @GetMapping("/getStatistics/{from}/{to}")
    public List<RecordDTO> getStatistics(@PathVariable("from")  Long from, @PathVariable("to") Long to){
        System.out.println(from);
        System.out.println(to);
        return AppManager.getRecords(from, to);

    }
    @GetMapping("/getStatistics")
    public List<RecordDTO> getStatistics(){
        long from = 0l;
        long to = 1647958696L;
        return AppManager.getRecords(from,to);
    }

}