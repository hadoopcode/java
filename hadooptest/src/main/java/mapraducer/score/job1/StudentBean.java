package mapraducer.score.job1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentBean implements WritableComparable<StudentBean> {
    private int classId;
    private int studentId;
    private String name;
    private Double chineseScore;
    private Double mathScore;
    private Double englishScore;
    private Double sum;
    private Double average;

    public int getClassId() {
        return classId;
    }

    public void setClassId(int classId) {
        this.classId = classId;
    }

    public int getStudentId() {
        return studentId;
    }

    public void setStudentId(int studentId) {
        this.studentId = studentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getChineseScore() {
        return chineseScore;
    }

    public void setChineseScore(Double chineseScore) {
        this.chineseScore = chineseScore;
    }

    public Double getMathScore() {
        return mathScore;
    }

    public void setMathScore(Double mathScore) {
        this.mathScore = mathScore;
    }

    public Double getEnglishScore() {
        return englishScore;
    }

    public void setEnglishScore(Double englishScore) {
        this.englishScore = englishScore;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(classId);
        dataOutput.writeInt(studentId);
        dataOutput.writeUTF(name);
        dataOutput.writeDouble(chineseScore);
        dataOutput.writeDouble(mathScore);
        dataOutput.writeDouble(englishScore);
        dataOutput.writeDouble(sum);
        dataOutput.writeDouble(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        classId = dataInput.readInt();
        studentId = dataInput.readInt();
        name = dataInput.readUTF();
        chineseScore = dataInput.readDouble();
        mathScore = dataInput.readDouble();
        englishScore = dataInput.readDouble();
        sum = dataInput.readDouble();
        average = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "StudentBean{" +
                "classId=" + classId +
                ", studentId=" + studentId +
                ", name='" + name + '\'' +
                ", chineseScore=" + chineseScore +
                ", mathScore=" + mathScore +
                ", englishScore=" + englishScore +
                ", sum=" + sum +
                ", average=" + average +
                '}';
    }

    @Override
    public int compareTo(StudentBean o) {
        return o.getSum().compareTo(this.getSum());
    }
}
