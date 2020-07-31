package mapraducer.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance(new Configuration());
        instance.setJarByClass(FlowCountDriver.class);
        instance.setMapperClass(FlowCountMapper.class);
        instance.setReducerClass(FlowCountReducer.class);
        instance.setPartitionerClass(MyPartition.class);
        instance.setNumReduceTasks(4);
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(FlowBean.class);
        instance.setOutputKeyClass(FlowBean.class);
        instance.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\phone"));
        FileOutputFormat.setOutputPath(instance,new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\flow1"));
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
