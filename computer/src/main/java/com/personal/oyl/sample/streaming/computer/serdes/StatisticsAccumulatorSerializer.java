package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.Statistics;
import com.personal.oyl.sample.streaming.computer.StatisticsAccumulator;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsAccumulatorSerializer implements Serializer<StatisticsAccumulator> {
    private static final String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, StatisticsAccumulator data) {

        if (null == data) {
            return null;
        }

        try {
            return data.json().getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing StatisticsAccumulator to byte[] due to unsupported encoding " + encoding);
        }
    }

}
