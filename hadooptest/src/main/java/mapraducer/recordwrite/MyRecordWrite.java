package mapraducer.recordwrite;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;


public class MyRecordWrite extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream at = null;
    private FSDataOutputStream other = null;
    private FileSystem fs = null;

    public MyRecordWrite(TaskAttemptContext job) throws IOException {
        fs = FileSystem.get(job.getConfiguration());
        at = fs.create(new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\log\\at.log"));
        other = fs.create(new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\output\\log\\other.log"));
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String s = value.toString();
        s = s + "\n";
        if (s.contains("at")) {
            at.write(s.getBytes());
        } else {
            other.write(s.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(at, other);
    }
}
