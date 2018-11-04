package com.sidd.learn.application;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamCountApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"Kafka-Stream-Word-Count");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> inputStream  = builder.stream("alpha");

        inputStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((mKey, mValue) -> mValue)
                .groupByKey()
                .count()
                .toStream()
                .to("beta",Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),properties);
        //addShutdownHook(streams);
        //System.out.println("STREAMS STATE AS" + streams);
        //System.out.println("STREAMS STATE AS" + streams.state());
        //System.out.println("STREAMS METADATA AS" + streams.allMetadata());
        streams.start();


    }

    private static void addShutdownHook(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
