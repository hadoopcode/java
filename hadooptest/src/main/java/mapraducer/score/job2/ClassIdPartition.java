package mapraducer.score.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClassIdPartition extends Partitioner<IntWritable, StudentBean> {

    @Override
    public int getPartition(IntWritable intWritable, StudentBean studentBean, int numPartitions) {
        int classId = intWritable.get();
        if (classId == 1303) {
            return 1;
        } else if (classId == 1304) {
            return 2;
        } else if (classId == 1305) {
            return 3;
        } else if (classId == 1306) {
            return 4;
        } else if (classId == 1307) {
            return 5;
        } else {
            return 0;
        }


    }
}
