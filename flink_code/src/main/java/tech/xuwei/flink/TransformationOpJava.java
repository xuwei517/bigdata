package tech.xuwei.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * 需求：
 * map()：对数据流中每个元素乘以2。
 * flatMap()：将数据流中的每行数据拆分为单词。
 * filter()：过滤出数据流中的偶数。
 * keyBy()：对数据流中的单词进行分组。
 * union()：对两个数据流中的数字进行合并。
 * connect()：将两个数据流中的用户信息关联到一起。
 *
 * Created by xuwei
 */
public class TransformationOpJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = getEnv();
        //map：对数据流中的每个元素乘以2
        //mapOp(env);
        //flatMap：将数据流中的每行数据拆分为单词
        //flatMapOp(env);
        //filter：过滤出数据流中的偶数
        //filterOp(env);
        //keyBy：对数据流中的单词进行分组
        //keyByOp(env);
        //union：对两个数据流中的数字进行合并
        //unionOp(env);
        //connect：将两个数据流中的用户信息关联到一起
        //connectOp(env);
        //执行程序
        env.execute("TransformationOpJava");
    }

    private static void connectOp(StreamExecutionEnvironment env) {
        //第1份数据流
        DataStreamSource<String> text1 = env.fromElements("user:tom,age:18");
        //第2份数据流
        DataStreamSource<String> text2 = env.fromElements("user:jack_age:18");

        //连接两个流
        ConnectedStreams<String, String> connectStream = text1.connect(text2);

        SingleOutputStreamOperator<String> resStream = connectStream.map(new CoMapFunction<String, String, String>() {

            //处理第1份数据流中的数据
            @Override
            public String map1(String value) throws Exception {
                return value.replace(",", "-");
            }

            //处理第2份数据流中的数据
            @Override
            public String map2(String value) throws Exception {
                return value.replace("_", "-");
            }
        });
        //使用一个线程执行打印操作
        resStream.print().setParallelism(1);
    }

    private static void unionOp(StreamExecutionEnvironment env) {
        //第1份数据流
        DataStreamSource<Integer> text1 = env.fromElements(1, 2, 3, 4, 5);
        //第2份数据流
        DataStreamSource<Integer> text2 = env.fromElements(6, 7, 8, 9, 10);

        //合并流
        DataStream<Integer> unionStream = text1.union(text2);

        //使用一个线程执行打印操作
        unionStream.print().setParallelism(1);
    }

    private static void keyByOp(StreamExecutionEnvironment env) {
        //在测试阶段，可以使用fromElements构造实时数据流
        DataStreamSource<String> text = env.fromElements("hello","you","hello","me");

        //处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountStream = text.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyByStream = wordCountStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> tup) throws Exception {
                return tup.f0;
            }
        });
        //使用一个线程执行打印操作
        keyByStream.print().setParallelism(1);
    }

    private static void filterOp(StreamExecutionEnvironment env) {
        //在测试阶段，可以使用fromElements构造实时数据流
        DataStreamSource<Integer> text = env.fromElements(1,2,3,4,5);
        //处理数据
        SingleOutputStreamOperator<Integer> numStream = text.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        //使用一个线程执行打印操作
        numStream.print().setParallelism(1);
    }

    private static void flatMapOp(StreamExecutionEnvironment env) {
        //在测试阶段，可以使用fromElements构造实时数据流
        DataStreamSource<String> text = env.fromElements("hello you","hello me");

        //处理数据
        SingleOutputStreamOperator<String> wordStream = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //使用一个线程执行打印操作
        wordStream.print().setParallelism(1);
    }

    private static void mapOp(StreamExecutionEnvironment env) {
        //在测试阶段，可以使用fromElements构造实时数据流
        DataStreamSource<Integer> text = env.fromElements(1,2,3,4,5);

        //处理数据
        SingleOutputStreamOperator<Integer> numStream = text.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });
        //使用一个线程执行打印操作
        numStream.print().setParallelism(1);
    }

    private static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
