package mapraducer.myinputfileformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyInputFileFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(MyInputFileFormatDriver.class);
        job.setInputFormatClass(MyInputFileFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.setInputPaths(job,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\threefile"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\threefile"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
