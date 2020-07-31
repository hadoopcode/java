package mapraducer.Wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String w : words) {
            text.set(w);
            intWritable.set(1);
            context.write(text,intWritable);
        }

    }
}
