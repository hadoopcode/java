package com.hp;

import com.hp.tool.HbaseToolRunner;
import org.apache.hadoop.util.ToolRunner;

public class HbaseMrApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HbaseToolRunner(), args);
    }
}
