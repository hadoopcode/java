package com.hp.tool;

import com.hp.mapper.ReadFileMapper;
import com.hp.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class HbaseToolRunner implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();

        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop101:9000/test"));
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob("Test:test2", InsertDataReducer.class,job);
        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {
    }

    public Configuration getConf() {
        return null;
    }
}
