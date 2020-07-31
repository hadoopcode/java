package customizeSource;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    /*
    * 定义从配置文件中读取的字段
    * */

    //两条数据之间发送的时间间隔
    private Long delay;
    private String field;

    public Status process() throws EventDeliveryException {
        try {
            HashMap<String, String> headerMap = new HashMap<String, String>();
            SimpleEvent event = new SimpleEvent();
            for (int i = 0; i < 5; i++) {
                event.setHeaders(headerMap);
                event.setBody((field + i).getBytes());
                this.getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {
        delay= context.getLong("delay");
        field= context.getString("field", "default_field");
    }
}
