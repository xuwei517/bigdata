package tech.xuwei.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 实时单词计数
 *
 * Created by xuwei
 */
public class StreamWordCountJava {
    public static void main(String[] args) throws Exception{
        //创建SparkConf配置对象
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("StreamWordCountJava");

        //创建StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //通过socket获取实时产生的数据
        JavaReceiverInputDStream<String> linesRDD = ssc.socketTextStream ("bigdata04", 9001);

        //对收到的数据使用空格进行拆分，拆分成单词
        JavaDStream<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //把每个单词转换为tuple2的形式
        JavaPairDStream<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //执行reduceByKey操作
        JavaPairDStream<String, Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        //将结果数据打印到控制台
        wordCountRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> pair) throws Exception {
                pair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    public void call(Tuple2<String, Integer> tup) throws Exception {
                        System.out.println(tup._1+"---"+tup._2);
                    }
                });
            }
        });

        //启动任务
        ssc.start();
        //等待任务停止
        ssc.awaitTermination();
    }
}
