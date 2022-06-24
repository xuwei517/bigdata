package tech.xuwei.flink.datacalcscala

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 1：统计全站双11当天实时GMV
 * 2：统计实时销量TopN的商品品类
 * Created by xuwei
 */
object StreamDataCalcScala {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //指定FlinkKafkaConsumer相关配置
    val topic = "order_detail"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092")
    prop.setProperty("group.id","con")
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)

    //指定Kafka作为source
    import org.apache.flink.api.scala._
    val text = env.addSource(kafkaConsumer)

    //解析订单数据，将数据打平，只保留需要用到的字段
    //goodsCount、goodsPrice、goodsType
    val orderStream = text.flatMap(line=>{
      val orderJson = JSON.parseObject(line)
      val orderDetal = orderJson.getJSONArray("detal")
      val res = new Array[(Long,Long,String)](orderDetal.size())
      for(i <- 0 until orderDetal.size()){
        val orderObj = orderDetal.getJSONObject(i)
        val goodsCount = orderObj.getLongValue("goodsCount")
        val goodsPrice = orderObj.getLongValue("goodsPrice")
        val goodsType = orderObj.getString("goodsType")
        res(i) = (goodsCount,goodsPrice,goodsType)
      }
      res
    })

    //过滤异常数据
    val filterStreram = orderStream.filter(_._1 > 0)

    //1. 统计全站“双十一”当天的实时GMV
    val gmvStream = filterStreram.map(tup=>tup._1 * tup._2)//计算单个商品的消费金融
    gmvStream.addSink(new GmvRedisSink("bigdata04",6380,"gmv"))


    //2.统计实时销量Top N的商品品类
    val topNStream = filterStreram.map(tup=>(tup._3,tup._1))//获取商品品类和购买的商品数量
      .keyBy(tup=>tup._1)                              //根据商品品类分组
      .timeWindow(Time.seconds(1))                   //设置时间窗口为1 s
      .sum(1)                         //指定tuple中的第2列，即商品数量

    topNStream.addSink(new TopNRedisSink("bigdata04",6380,"goods_type"))

    env.execute("StreamDataCalcScala")

  }

}
