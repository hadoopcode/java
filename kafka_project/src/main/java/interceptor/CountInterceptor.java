package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String,String> {
    private Long successNum = 0L;
    private Long failNum = 0L;
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("打印");
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.key(), producerRecord.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            successNum++;
        } else {
            failNum++;
        }
    }

    public void close() {
        System.out.println("关闭");
    }

    public void configure(Map<String, ?> map) {

    }
}
