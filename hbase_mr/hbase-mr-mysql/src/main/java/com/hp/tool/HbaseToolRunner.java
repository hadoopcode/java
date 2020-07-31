package com.hp.tool;

import com.hp.HbaseMrApplication;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HbaseToolRunner implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HbaseMrApplication.class);


        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
