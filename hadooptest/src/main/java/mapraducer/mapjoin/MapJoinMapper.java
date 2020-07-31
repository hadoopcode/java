package mapraducer.mapjoin;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> hashMap = new HashMap<>();
    private Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] split = line.split("\t");
            hashMap.put(split[0], split[1]);
        }
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String pdName = hashMap.get(fields[1]);
        text.set(String.format("%s\t%s", value.toString(), pdName));
        context.write(text,NullWritable.get());
    }
}
