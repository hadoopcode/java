package mapraducer.score.job1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ScoreMapper.class);
        job.setMapOutputKeyClass(StudentBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\score"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\score"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
