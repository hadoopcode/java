package mapraducer.utils;

import mapraducer.myInputFile.SequenceFileDriver;
import mapraducer.myInputFile.SequenceFileMapper;
import mapraducer.myInputFile.SequenceFileReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class DriverUtils {
    public static Job setJobParam(Class<?> jar,Class<?> mapperClass,Class<?> reducerClass,Class<?> mapOutputKeyClass,Class<?> mapOutputValueClass,Class<?>outputKeyClass,Class<?> outputValueClass) throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(jar);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        return job;
    }
}
