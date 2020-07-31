package com.hp.MyRecordWriter;

import com.hp.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ToMysqlRecordWriter extends RecordWriter<Text, CacheData> {

    public void write(Text text, CacheData cacheData) throws IOException, InterruptedException {

    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}
