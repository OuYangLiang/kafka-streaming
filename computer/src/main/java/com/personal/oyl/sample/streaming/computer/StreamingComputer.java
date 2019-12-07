package com.personal.oyl.sample.streaming.computer;

import com.personal.oyl.sample.streaming.computer.serdes.StatisticsAccumulatorSerde;
import com.personal.oyl.sample.streaming.computer.serdes.UserStatisticsSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author OuYang Liang
 * @since 2019-09-03
 */
public class StreamingComputer {
    public static void main(String[] args) {
        new StreamingComputer().start();
    }

    public void start() {
        KafkaStreams streams = new KafkaStreams(this.topology(), this.settings());

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("kafka-streaming-app-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> stream = builder.stream("order_queue",
                Consumed.with(Serdes.Integer(), Serdes.String()));

        // 任意时刻，某客户在某分钟内的下单量、下单金额，实时查询
        stream.groupByKey().windowedBy(TimeWindows
                    .of(Duration.ofMinutes(1))
//                    .advanceBy(Duration.ofMinutes(1))
                    .grace(Duration.ofSeconds(10))
                ).aggregate(
                    UserStatistics::new,
                    (k, v, userStatistics) -> {
                        Order order = Order.fromJson(v);
                        UserStatistics statistics = new UserStatistics();
                        statistics.setCustId(k);
                        statistics.setNumOfOrders(userStatistics.getNumOfOrders() + 1);
                        statistics.setOrderAmt(userStatistics.getOrderAmt().add(order.getPayAmt()));
                        return statistics;
                    },
                    Materialized.<Integer, UserStatistics, WindowStore<Bytes, byte[]>>as("aggregated-per-user").withValueSerde(new UserStatisticsSerde())
                ).mapValues(
                    (k,v) -> {
                        UserStatistics statistics = new UserStatistics();
                        statistics.setCustId(k.key());
                        statistics.setOrderAmt(v.getOrderAmt());
                        statistics.setNumOfOrders(v.getNumOfOrders());
//                        statistics.setMinute(formatTsStart(k.window().start()) + "~" + formatTsEnd(k.window().end()));
                        statistics.setMinute(formatTs(k.window().start()));
                        return statistics.json();
                    }
                ).suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(null, v))
                .to("user_statistics", Produced.with(null, Serdes.String()));

        // 任意时刻，某分钟内的下单量、下单金额，下单客户数，实时查询
        stream.groupBy((k,v) -> 1, Grouped.with(Serdes.Integer(), Serdes.String())).windowedBy(TimeWindows
                .of(Duration.ofMinutes(1))
//                .advanceBy(Duration.ofMinutes(1))
                .grace(Duration.ofSeconds(10))
        ).aggregate(
                StatisticsAccumulator::new,
                (k, v, statistics) -> {
                    Order order = Order.fromJson(v);
                    StatisticsAccumulator rlt = new StatisticsAccumulator();
                    rlt.setNumOfOrders(statistics.getNumOfOrders() + 1);
                    rlt.setOrderAmt(statistics.getOrderAmt().add(order.getPayAmt()));
                    rlt.getOrderedCustId().add(order.getCustId());
                    rlt.getOrderedCustId().addAll(statistics.getOrderedCustId());
                    return rlt;
                },
                Materialized.<Integer, StatisticsAccumulator, WindowStore<Bytes, byte[]>>as("aggregated-globally").withValueSerde(new StatisticsAccumulatorSerde())
        ).mapValues(
                (k,v) -> {
                    Statistics rlt = v.toStatistics();
                    rlt.setMinute(formatTs(k.window().start()));
                    return rlt.json();
                }
        ).suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(null, v))
                .to("minute_statistics", Produced.with(null, Serdes.String()));

        return builder.build();
    }


    private Properties settings() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-app"); // it is explained as kafka consumer group id
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        /*
         * For such KTable instances, the record cache is used for:
         * Internal caching and compacting of output records before they are written by the underlying stateful processor node to its internal state stores.
         * Internal caching and compacting of output records before they are forwarded from the underlying stateful processor node to any of its downstream processor nodes.
         */
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return props;
    }

    private String formatTs(long ts) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(ts);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return sdf.format(c.getTime());
    }

    private String formatTsStart(long ts) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(ts);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(c.getTime());
    }

    private String formatTsEnd(long ts) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(ts);

        SimpleDateFormat sdf = new SimpleDateFormat("mm");
        return sdf.format(c.getTime());
    }
}
