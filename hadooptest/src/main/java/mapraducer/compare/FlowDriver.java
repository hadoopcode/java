package mapraducer.compare;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job instance = Job.getInstance(new Configuration());
        instance.setJarByClass(FlowDriver.class);
        instance.setMapperClass(FlowMapper.class);
        instance.setReducerClass(FlowReducer.class);
        instance.setMapOutputKeyClass(FlowBean.class);
        instance.setMapOutputValueClass(Text.class);
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\phone"));
        FileOutputFormat.setOutputPath(instance,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\flow2"));
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
