package mapraducer.myinputfileformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean isProgress=true;
    private FSDataInputStream inputStream;
    private Path path;
    private FileSplit fs;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs = (FileSplit) split;
        path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        inputStream = fileSystem.open(path);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            key.set(path.toString());
            byte[] contents = new byte[(int) fs.getLength()];
            inputStream.read(contents);
            value.set(contents,0,contents.length);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProgress?0:1;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}
