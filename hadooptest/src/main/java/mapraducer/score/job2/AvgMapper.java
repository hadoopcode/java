package mapraducer.score.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgMapper extends Mapper<LongWritable, Text, IntWritable, StudentBean> {
    private StudentBean studentBean = new StudentBean();
    private IntWritable intWritable = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\s+");
        studentBean.setClassId(Integer.parseInt(fields[0]));
        studentBean.setChineseScore(Float.parseFloat(fields[3]));
        studentBean.setMathScore(Float.parseFloat(fields[4]));
        studentBean.setEnglishScore(Float.parseFloat(fields[5]));
        intWritable.set(Integer.parseInt(fields[0]));
        context.write(intWritable,studentBean);
    }
}
