package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.Statistics;
import com.personal.oyl.sample.streaming.computer.StatisticsAccumulator;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsAccumulatorDeserializer implements Deserializer<StatisticsAccumulator> {
    private static final String encoding = "UTF8";

    @Override
    public StatisticsAccumulator deserialize(String topic, byte[] data) {

        if (null == data) {
            return null;
        }

        try {
            return StatisticsAccumulator.fromJson(new String(data, encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to StatisticsAccumulator due to unsupported encoding " + encoding);
        }
    }
}
