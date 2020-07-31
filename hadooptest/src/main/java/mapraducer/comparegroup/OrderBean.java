package mapraducer.comparegroup;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private double price;
    private String productId;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
        dataOutput.writeUTF(productId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
        productId = dataInput.readUTF();
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = Integer.compare(o.getOrderId(), this.getOrderId());
        if (compare == 0) {
            return Double.compare(o.getPrice(), this.getPrice());
        } else {
            return compare;
        }
    }
}
