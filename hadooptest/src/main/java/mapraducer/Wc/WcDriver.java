package mapraducer.Wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job instance = Job.getInstance(new Configuration());

        instance.setJarByClass(WcDriver.class);
        instance.setMapperClass(WcMap.class);
        instance.setReducerClass(WcReducer.class);
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(instance, new Path(args[0]));
        FileOutputFormat.setOutputPath(instance,new Path(args[1]));

        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
