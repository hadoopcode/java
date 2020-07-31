package mapraducer.reducerjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RjReducer extends Reducer<TabBean, NullWritable, TabBean, NullWritable> {
    @Override
    protected void reduce(TabBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }
    }
}
