package mapraducer.score.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AvgReducer extends Reducer<IntWritable, StudentBean, Text, NullWritable> {
    private Text text = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<StudentBean> values, Context context) throws IOException, InterruptedException {
        Iterator<StudentBean> iterator = values.iterator();
        float chineseSum = 0;
        float mathSum = 0;
        float englishSum = 0;
        int count = 0;
        for (StudentBean studentBean : values) {
            chineseSum += studentBean.getChineseScore();
            mathSum += studentBean.getMathScore();
            englishSum += studentBean.getEnglishScore();
            count++;
        }
        text.set(String.format("%d\t%f\t%f\t%f",key.get(),chineseSum/count,mathSum/count,englishSum/count));
        context.write(text, NullWritable.get());
    }
}
