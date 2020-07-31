package com.hp.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\t");
        String rowkey = fields[1];
        byte[] bytes = Bytes.toBytes(rowkey);
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(fields[0]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[2]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("num"), Bytes.toBytes(fields[3]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("num1"), Bytes.toBytes(fields[4]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("site"), Bytes.toBytes(fields[5]));
        context.write(new ImmutableBytesWritable(bytes),put);
    }
}
