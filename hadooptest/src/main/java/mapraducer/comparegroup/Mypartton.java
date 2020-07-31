package mapraducer.comparegroup;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Mypartton extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        if (orderBean.getOrderId() == 3) {
            return 3;
        } else if (orderBean.getOrderId() == 2) {
            return 2;
        } else if (orderBean.getOrderId() == 1) {
            return 1;
        } else {
            return 0;
        }
    }
}
