package mapraducer.comparegroup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        orderBean.setOrderId(Integer.parseInt(split[0]));
        orderBean.setPrice(Double.parseDouble(split[2]));
        orderBean.setProductId(split[1]);
        context.write(orderBean,NullWritable.get());
    }
}
