package customizeSource;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);
    private String prefix;
    private String suffix;

    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Transaction transaction = ch.getTransaction();
        Event event;
        Status status;
        transaction.begin();
        while ((event = ch.take()) == null);
        try {
            LOG.info(prefix + new String(event.getBody()) + suffix);
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            transaction.rollback();
            status = Status.BACKOFF;
        }finally {
            transaction.close();
        }
        return status;
    }

    public void configure(Context context) {
        prefix = context.getString("prefix", "hello:");
        suffix = context.getString("suffix");
    }
}
