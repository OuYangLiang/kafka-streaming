package com.personal.oyl.sample.stream.sink;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author OuYang Liang
 * @since 2019-12-05
 */
public class Sink {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        DBPersister persister = new DBPersister();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("user_statistics", "minute_statistics"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofDays(1));
                for (ConsumerRecord<String, String> record : records) {

                    if (record.topic().equals("user_statistics")) {
                        String value = record.value();
                        UserStatistics statistics = UserStatistics.fromJson(value);
                        persister.save(statistics);
                    } else if (record.topic().equals("minute_statistics")) {
                        String value = record.value();
                        Statistics statistics = Statistics.fromJson(value);
                        persister.save(statistics);
                    }

                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException | SQLException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
