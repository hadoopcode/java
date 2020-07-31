import com.sun.org.apache.xalan.internal.xsltc.runtime.Operators;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopTest {
    @Test
    public void testMkdir() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.79.101:9000"), new Configuration(), "root");
        fs.mkdirs(new Path("/hp/test"));
        fs.close();
    }

    @Test
    public void testCopyFormLocalFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.79.101:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\log4j.properties"),new Path("/hp/test/log4j.properties"));
        fs.close();
    }

    @Test
    public void testCopyToLocal(){
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.79.101:9000"), new Configuration(), "root");
            fs.copyToLocalFile(false, new Path("/hp/test/log4j.properties"), new Path("F:/log4j.properties"));
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.79.101:9000"), new Configuration(), "root");
        fs.delete(new Path("/hp/test/log4j.properties"),true);
        fs.close();
    }
    
    @Test
    public void testListFile() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","3");
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.79.101:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("F:\\project\\java\\hadooptest\\src\\main\\resources\\log4j.properties"),new Path("/hp/test/log4j.properties"));
        System.out.println("-----------------------------------------------------------------------");
        RemoteIterator<LocatedFileStatus> lfs = fs.listFiles(new Path("/"), true);
        while (lfs.hasNext()) {
            LocatedFileStatus next = lfs.next();
            System.out.print(next.getPath().getName());
        }
    }
}
