package mapraducer.score.job2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class AvgDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();

        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(StudentBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(6);
        job.setPartitionerClass(ClassIdPartition.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\score"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\scoreAvg"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
