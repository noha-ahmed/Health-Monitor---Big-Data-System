package map_reduce;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ScheduleJob implements Job {

    private Logger log = Logger.getLogger(ScheduleJob.class);

    public void execute(JobExecutionContext jExeCtx) throws JobExecutionException {
        //call function here
    }

}