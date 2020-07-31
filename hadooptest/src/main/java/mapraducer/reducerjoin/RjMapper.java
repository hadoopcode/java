package mapraducer.reducerjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RjMapper extends Mapper<LongWritable, Text, TabBean, NullWritable> {
    private TabBean tabBean = new TabBean();
    private String filename;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        this.filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (filename.equals("order.txt")) {
            tabBean.setOrderId(fields[0]);
            tabBean.setProductId(fields[1]);
            tabBean.setAmount(Integer.parseInt(fields[2]));
            tabBean.setPname(null);
            tabBean.setFlag(filename);
        } else {
            tabBean.setProductId(fields[0]);
            tabBean.setPname(fields[1]);
            tabBean.setFlag(filename);
            tabBean.setAmount(0);
            tabBean.setOrderId(null);
        }
    }
}
