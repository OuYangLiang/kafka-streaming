package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.UserStatistics;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class UserStatisticsDeserializer implements Deserializer<UserStatistics> {
    private static final String encoding = "UTF8";

    @Override
    public UserStatistics deserialize(String topic, byte[] data) {

        if (null == data) {
            return null;
        }

        try {
            return UserStatistics.fromJson(new String(data, encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to UserStatistics due to unsupported encoding " + encoding);
        }
    }
}
