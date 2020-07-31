package mapraducer.etl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance();

        instance.setMapperClass(ETLMapper.class);
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\weblog"));
        FileOutputFormat.setOutputPath(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\weblog"));
        boolean b = instance.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
