package mapraducer.score.job3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopCompartor extends WritableComparator {

    public TopCompartor() {
        super(StudentBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StudentBean a1 = (StudentBean) a;
        StudentBean b1 = (StudentBean) b;
        return a1.getClassId() - b1.getClassId();
    }
}
