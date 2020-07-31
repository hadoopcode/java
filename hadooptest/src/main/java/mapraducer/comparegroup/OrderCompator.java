package mapraducer.comparegroup;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderCompator extends WritableComparator {

    public OrderCompator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        return Integer.compare(a1.getOrderId(), b1.getOrderId());
    }
}
