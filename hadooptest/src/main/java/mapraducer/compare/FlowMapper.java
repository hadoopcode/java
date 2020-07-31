package mapraducer.compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private FlowBean flowBean = new FlowBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        text.set(fields[1]);
        flowBean.setDownFlow(Long.parseLong(fields[fields.length - 3]));
        flowBean.setUpFlow(Long.parseLong(fields[fields.length - 2]));
        flowBean.setSumFlow(flowBean.getUpFlow()+flowBean.getDownFlow());
        context.write(flowBean,text);
    }
}
