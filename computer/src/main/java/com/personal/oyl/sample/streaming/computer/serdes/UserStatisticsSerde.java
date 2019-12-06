package com.personal.oyl.sample.streaming.computer.serdes;

import com.personal.oyl.sample.streaming.computer.UserStatistics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author OuYang Liang
 * @since 2019-12-04
 */
public class UserStatisticsSerde implements Serde<UserStatistics> {

    @Override
    public Serializer<UserStatistics> serializer() {
        return new UserStatisticsSerializer();
    }

    @Override
    public Deserializer<UserStatistics> deserializer() {
        return new UserStatisticsDeserializer();
    }
}
