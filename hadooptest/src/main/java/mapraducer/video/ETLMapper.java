package mapraducer.video;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Text text = new Text();
    private StringBuilder sb = new StringBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String result = etl(line);
        if (result == null) {
            context.getCounter("ETL", "false").increment(1);
        } else {
            context.getCounter("ETL","true").increment(1);
            text.set(result);
            context.write(text,NullWritable.get());
        }
    }

    public String etl(String line){
        String[] fields = line.split("\t");
        if (fields.length < 9) {
            return null;
        }
        sb.delete(0, fields.length);

        fields[3] = fields[3].replace(" ", "");
        for (int i = 0; i < fields.length; i++) {
            if (i < 9) {
                sb.append(fields[i]).append("\t");
            } else if (fields.length - 1 == 9) {
                sb.append(fields[i]);
            } else {
                sb.append(fields[i]).append("&");
            }
        }
        return sb.toString();
    }
}
