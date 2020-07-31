package mapraducer.myInputFile;

import mapraducer.utils.DriverUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = DriverUtils.setJobParam(SequenceFileDriver.class, SequenceFileMapper.class, SequenceFileReducer.class, Text.class, BytesWritable.class, Text.class, BytesWritable.class);
        job.setInputFormatClass(JoinFileInputFileFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\threefile"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\threefile"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
