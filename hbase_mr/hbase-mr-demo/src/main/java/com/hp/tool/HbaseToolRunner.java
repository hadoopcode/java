package com.hp.tool;

import com.hp.HbaseMrApplication;
import com.hp.mapper.ScanDataMapper;
import com.hp.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HbaseToolRunner implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HbaseMrApplication.class);

        TableMapReduceUtil.initTableMapperJob("student", new Scan(), ScanDataMapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("user", InsertDataReducer.class, job);

        boolean b = job.waitForCompletion(true);
        return b ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
