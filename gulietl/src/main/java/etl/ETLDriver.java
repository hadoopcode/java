package etl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\project\\java\\gulietl\\src\\main\\resources\\video"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\project\\java\\gulietl\\src\\main\\resources\\video_etl"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
