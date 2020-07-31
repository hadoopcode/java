package mapraducer.score.job3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopReducer extends Reducer<StudentBean,Text, Text,NullWritable> {
    @Override
    protected void reduce(StudentBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 1;
        for (Text value : values) {
            if (count <= 5) {
                count++;
                context.write(value,NullWritable.get());
            }
        }
    }
}
