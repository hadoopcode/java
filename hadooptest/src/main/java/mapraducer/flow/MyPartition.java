package mapraducer.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String s = text.toString();
        int postion;
        if ("136".equals(s.substring(0, 3))) {
            postion = 0;
        } else if ("137".equals(s.substring(0, 3))) {
            postion = 1;
        } else if ("138".equals(s.substring(0, 3))) {
            postion = 2;
        } else {
            postion = 3;
        }
        return postion;
    }
}
