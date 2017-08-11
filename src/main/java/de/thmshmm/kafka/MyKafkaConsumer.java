package de.thmshmm.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Thomas Hamm on 24.07.17.
 */
public class MyKafkaConsumer implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    public MyKafkaConsumer(Properties props) {
         consumer = new KafkaConsumer(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList("test"));

            while (!closed.get()) {
                ConsumerRecords records = consumer.poll(10000);

                Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();

                while(recordIterator.hasNext()) {
                    System.out.println(recordIterator.next().value());
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
