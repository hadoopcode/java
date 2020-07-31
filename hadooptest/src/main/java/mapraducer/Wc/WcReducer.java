package mapraducer.Wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable intWritable : values) {
            total = +intWritable.get();
            intWritable.set(total);
            context.write(key,intWritable);
        }
    }
}
