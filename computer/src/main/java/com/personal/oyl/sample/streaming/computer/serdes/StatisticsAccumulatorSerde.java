package com.personal.oyl.sample.streaming.computer.serdes;


import com.personal.oyl.sample.streaming.computer.Statistics;
import com.personal.oyl.sample.streaming.computer.StatisticsAccumulator;
import org.apache.kafka.common.serialization.Serde;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsAccumulatorSerde implements Serde<StatisticsAccumulator> {

    public StatisticsAccumulatorSerializer serializer() {
        return new StatisticsAccumulatorSerializer();
    }

    public StatisticsAccumulatorDeserializer deserializer() {
        return new StatisticsAccumulatorDeserializer();
    }
}
