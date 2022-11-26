package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class kafkaJob {

    public static void main(String[] args)  throws Exception{
        final String bootstrapServers = args.length > 0 ? args[0] : "broker:9092";

        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(Arrays.asList("event1", "event2"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source");

        stream.print();

        env.execute();
    }
}
