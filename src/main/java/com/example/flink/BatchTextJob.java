package com.example.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchTextJob {

    public static void main(String[] args) throws Exception{
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
}
