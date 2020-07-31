package mapraducer.recordwrite;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance();
        instance.setJarByClass(MyRecordWrite.class);
//        instance.setOutputKeyClass(LongWritable.class);
//        instance.setOutputValueClass(Text.class);
        instance.setOutputFormatClass(MyFileOutPutFormat.class);
        FileInputFormat.setInputPaths(instance,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\log"));
        FileOutputFormat.setOutputPath(instance,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\log"));
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
