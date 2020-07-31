package mapraducer.mapjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance= Job.getInstance();
        instance.setJarByClass(MapJoinDriver.class);
        instance.setMapperClass(MapJoinMapper.class);
        instance.addCacheFile(URI.create("file:///F://project//java//hadooptest//src//main//resources//input//reducerjoin//pd//pd.txt"));
        instance.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\input\\reducerjoin\\order"));
        FileOutputFormat.setOutputPath(instance, new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\join"));
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
