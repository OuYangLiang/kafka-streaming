package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.Statistics;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class StatisticsSerializer implements Serializer<Statistics> {
    private static final String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Statistics data) {

        if (null == data) {
            return null;
        }

        try {
            return data.json().getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing Statistics to byte[] due to unsupported encoding " + encoding);
        }
    }

}
