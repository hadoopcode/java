package mapraducer.score.job3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance();

        instance.setMapperClass(TopMapper.class);
        instance.setReducerClass(TopReducer.class);

        instance.setMapOutputKeyClass(StudentBean.class);
        instance.setMapOutputValueClass(Text.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(NullWritable.class);

        instance.setGroupingComparatorClass(TopCompartor.class);

        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\score"));
        FileOutputFormat.setOutputPath(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\score3"));

        boolean b = instance.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
