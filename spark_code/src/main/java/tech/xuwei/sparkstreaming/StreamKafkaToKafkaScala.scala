package tech.xuwei.sparkstreaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming读写Kafka
 *
 * Created by xuwei
 */
object StreamKafkaToKafkaScala {
  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamKafkaScala")
    val ssc = new StreamingContext(conf, Seconds(5))

    //指定Kafka的配置信息
    val kafkaParams = Map[String,Object](
      //Kafka的broker地址信息
      "bootstrap.servers"->"bigdata01:9092,bigdata02:9092,bigdata03:9092",
      //key的序列化类型
      "key.deserializer"->classOf[StringDeserializer],
      //value的序列化类型
      "value.deserializer"->classOf[StringDeserializer],
      //消费者组ID
      "group.id"->"con_1",
      //消费策略
      "auto.offset.reset"->"latest",
      //自动提交offset
      "enable.auto.commit"->(true: java.lang.Boolean)
    )
    //指定要读取的topic的名称
    val topics = Array("t_in")

    //获取消费Kafka的数据流
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //处理数据
    val mapDStream = kafkaDStream.map(_.value())
    //将数据写入Kafka
    mapDStream
      .foreachRDD(rdd=>{
        rdd.foreachPartition(it=>{
          //组装输出Kafka配置信息
          val properties = new Properties()
          properties.put("bootstrap.servers","bigdata01:9092,bigdata02:9092, bigdata03:9092")
          properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
          properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
          val producer = new KafkaProducer[String, String](properties)
          it.foreach(line=>{
            producer.send(new ProducerRecord("t_out",line))
          })
          producer.close()
        })
      })

    //启动任务
    ssc.start()
    //等待任务停止
    ssc.awaitTermination()
  }

}
