package com.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamJob {

    public static void main(String[] args)  throws Exception{
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringDataSource = env.readTextFile("input/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> outputStreamOperator = stringDataSource.flatMap((String line, Collector<Tuple2<String,Long>> out)->{
            String[] words = line.split(" ");
            for(String word:words){
                out.collect(Tuple2.of(word,1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //4. 按照word进行分组
        KeyedStream<Tuple2<String, Long>,String> wordAndOneGroup = outputStreamOperator.keyBy(data->data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> aggregateOperator =  wordAndOneGroup.sum(1);

        aggregateOperator.print();

        env.execute();
    }
}
