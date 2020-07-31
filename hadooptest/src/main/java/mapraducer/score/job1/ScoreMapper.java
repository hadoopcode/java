package mapraducer.score.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text, StudentBean, NullWritable> {

    private StudentBean studentBean = new StudentBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        studentBean.setClassId(Integer.parseInt(fields[0]));
        studentBean.setStudentId(Integer.parseInt(fields[1]));
        studentBean.setName(fields[2]);
        studentBean.setChineseScore(Double.parseDouble(fields[3]));
        studentBean.setMathScore(Double.parseDouble(fields[4]));
        studentBean.setEnglishScore(Double.parseDouble(fields[5]));
        studentBean.setSum(studentBean.getChineseScore()+studentBean.getMathScore()+studentBean.getEnglishScore());
        studentBean.setAverage(studentBean.getSum()/3);
        context.write(studentBean,NullWritable.get());
    }
}
