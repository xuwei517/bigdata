package tech.xuwei.sparkstreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

/**
 * SparkStreaming读写Kafka
 *
 * Created by xuwei
 */
public class StreamKafkaToKafkaJava {
    public static void main(String[] args) throws Exception{
        //创建StreamingContext，指定读取数据的时间间隔为5 s
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("StreamKafkaJava");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //指定Kafka的配置信息
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers","bigdata01:9092,bigdata02:9092, bigdata03:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id","con_1");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",true);


        //指定要读取的topic名称
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("t_in");

        //获取消费Kafka的数据流
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        //处理数据
        JavaDStream<String> mapDStream = kafkaStream.map(new Function<ConsumerRecord<String, String>, String>() {
            public String call(ConsumerRecord<String, String> record)
                    throws Exception {
                return record.value();
            }
        });
        //将数据写入Kafka
        mapDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    public void call(Iterator<String> iterator) throws Exception {
                        //组装输出Kafka配置信息
                        Properties prop = new Properties();
                        prop.put("bootstrap.servers","bigdata01:9092, bigdata02:9092,bigdata03:9092");
                        prop.put("key.serializer", StringSerializer.class.getName());
                        prop.put("value.serializer", StringSerializer.class.getName());
                        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
                        while(iterator.hasNext()){
                            String line = iterator.next();
                            producer.send(new ProducerRecord<String, String>("t_out",line));
                        }
                        producer.close();
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
