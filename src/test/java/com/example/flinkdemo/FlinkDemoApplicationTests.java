package com.example.flinkdemo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class FlinkDemoApplicationTests {

    /**
     * 批处理方式
     * @throws Exception
     */
    @Test
    void batch() throws Exception {
        //1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 读取文件数据
        DataSource<String> stringDataSource = env.readTextFile("input/words.txt");
        //3. 将数据进行分词，转成二元数组
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = stringDataSource.flatMap((String line, Collector<Tuple2<String,Long>> out)->{
            String[] words = line.split(" ");
            for(String word:words){
                out.collect(Tuple2.of(word,1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = flatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> aggregateOperator =  wordAndOneGroup.sum(1);

        aggregateOperator.print();
    }


    /**
     * 有界流处理
     * @throws Exception
     */
    @Test
    void stream() throws Exception {

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

    /**
     * 无界流处理
     * @throws Exception
     */
    @Test
    void socket() throws Exception{
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringDataSource = env.socketTextStream("localhost",7777);
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
