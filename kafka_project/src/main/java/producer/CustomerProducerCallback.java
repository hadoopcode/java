package producer;

import com.sun.corba.se.impl.orbutil.ObjectUtility;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CustomerProducerCallback {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.79.101:9092");
//        properties.put("advertised.listeners", "192.168.79.101:9092,192.168.79.102:9092,192.168.79.103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500000);

        List<String> list = Arrays.asList("interceptor.CountInterceptor", "interceptor.TimeInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("second", Integer.toString(i), Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("recordMetadata.topic() = " + recordMetadata.topic());
                        System.out.println("recordMetadata.offset() = " + recordMetadata.offset());
                        System.out.println("recordMetadata.partition() = " + recordMetadata.partition());
                        System.out.println("***************************************************************");
                    } else {
                        e.printStackTrace();
                        System.out.println("发送失败");
                    }
                }
            });
        }
        producer.close();
    }
}
