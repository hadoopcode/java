package mapraducer.score.job2;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
public class StudentBean implements WritableComparable<StudentBean> {
    private int classId;
    private float chineseScore;
    private float mathScore;
    private float englishScore;


    @Override
    public int compareTo(StudentBean o) {
        return Integer.compare(o.classId, getClassId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(classId);
        out.writeFloat(chineseScore);
        out.writeFloat(mathScore);
        out.writeFloat(englishScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        classId = in.readInt();
        chineseScore = in.readFloat();
        mathScore = in.readFloat();
        englishScore = in.readFloat();
    }
}
