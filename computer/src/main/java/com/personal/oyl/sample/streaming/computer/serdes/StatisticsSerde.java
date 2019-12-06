package com.personal.oyl.sample.streaming.computer.serdes;


import com.personal.oyl.sample.streaming.computer.Statistics;
import org.apache.kafka.common.serialization.Serde;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsSerde implements Serde<Statistics> {

    public StatisticsSerializer serializer() {
        return new StatisticsSerializer();
    }

    public StatisticsDeserializer deserializer() {
        return new StatisticsDeserializer();
    }
}
