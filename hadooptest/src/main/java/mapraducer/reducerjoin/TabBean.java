package mapraducer.reducerjoin;

import lombok.Data;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
public class TabBean implements WritableComparable<TabBean> {
    private String orderId;
    private String productId;
    private String pname;
    private int amount;
    private String flag;

    public TabBean() {
        super();
    }

    public TabBean(String orderId, String productId, String pname, int amount, String flag) {
        this.orderId = orderId;
        this.productId = productId;
        this.pname = pname;
        this.amount = amount;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(pname);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.productId = dataInput.readUTF();
        this.pname = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.flag = dataInput.readUTF();
    }

    @Override
    public int compareTo(TabBean o) {
        int i = this.productId.compareTo(o.productId);
        if (i == 0) {
            return this.pname.compareTo(o.pname);
        } else {
            return i;
        }

    }

    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount;
    }
}
