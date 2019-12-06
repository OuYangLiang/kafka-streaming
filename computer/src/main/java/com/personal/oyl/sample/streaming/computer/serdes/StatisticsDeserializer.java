package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.Statistics;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsDeserializer implements Deserializer<Statistics> {
    private static final String encoding = "UTF8";

    @Override
    public Statistics deserialize(String topic, byte[] data) {

        if (null == data) {
            return null;
        }

        try {
            return Statistics.fromJson(new String(data, encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to Statistics due to unsupported encoding " + encoding);
        }
    }
}
