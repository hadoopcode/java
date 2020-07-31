package mapraducer.score.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopMapper extends Mapper<LongWritable, Text, StudentBean, Text> {
    StudentBean studentBean = new StudentBean();
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] field = value.toString().split("\\s+");
        studentBean.setClassId(Integer.parseInt(field[0]));
        studentBean.setStudentId(Integer.parseInt(field[1]));
        studentBean.setName(field[2]);
        int sum = 0;
        for (int i = 3; i < field.length; i++) {
            sum += Integer.parseInt(field[i]);
        }
        studentBean.setSumScore(sum);
        text.set(studentBean.toString());

        context.write(studentBean, text);

    }
}
