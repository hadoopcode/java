package com.hp.outputformat;

import com.hp.MyRecordWriter.ToMysqlRecordWriter;
import com.hp.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ToMysqlOutputFormat extends FileOutputFormat<Text, CacheData> {
    public RecordWriter<Text, CacheData> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ToMysqlRecordWriter();
    }

}
