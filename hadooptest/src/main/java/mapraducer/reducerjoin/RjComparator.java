package mapraducer.reducerjoin;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RjComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TabBean a1 = (TabBean) a;
        TabBean b1 = (TabBean) b;
        return a1.getProductId().compareTo(b1.getProductId());
    }
}
