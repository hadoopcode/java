package mapraducer.score.job3;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class StudentBean implements WritableComparable<StudentBean> {
    private int classId;
    private int studentId;
    private String name;
    private int sumScore;


    @Override
    public int compareTo(StudentBean o) {
        int compare = Integer.compare(o.getClassId(), getClassId());
        if (compare == 0) {
            return Integer.compare(o.getSumScore(), getSumScore());
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(classId);
        out.writeInt(studentId);
        out.writeUTF(name);
        out.writeInt(sumScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        classId = in.readInt();
        studentId = in.readInt();
        name = in.readUTF();
        sumScore = in.readInt();
    }
}
