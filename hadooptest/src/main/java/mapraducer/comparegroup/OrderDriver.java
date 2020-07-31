package mapraducer.comparegroup;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setGroupingComparatorClass(OrderCompator.class);
        job.setPartitionerClass(Mypartton.class);
        job.setNumReduceTasks(4);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\comparegroup"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\comparegroup\\"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
