package mapraducer.reducerjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RjDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance();
        instance.setJarByClass(RjDriver.class);

        instance.setMapperClass(RjMapper.class);
        instance.setReducerClass(RjReducer.class);

        instance.setGroupingComparatorClass(RjComparator.class);

        instance.setMapOutputKeyClass(TabBean.class);
        instance.setMapOutputValueClass(NullWritable.class);

        instance.setOutputKeyClass(TabBean.class);
        instance.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\reducerjoin\\mix"));
        FileOutputFormat.setOutputPath(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\rj"));

        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
