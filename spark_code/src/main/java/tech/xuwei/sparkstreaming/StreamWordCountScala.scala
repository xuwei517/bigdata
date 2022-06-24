package tech.xuwei.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 实时单词计数
 *
 * Created by xuwei
 */
object StreamWordCountScala {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置对象
    val conf = new SparkConf()
      //注意：此处的local[2]表示启动2个进程，一个进程负责读取数据源的数据，另一个进程负责处理数据
      .setMaster("local[2]")
      .setAppName("StreamWordCountScala")

    //创建StreamingContext，指定数据处理间隔为5 s
    val ssc = new StreamingContext(conf, Seconds(5))

    //通过socket获取实时产生的数据
    val linesRDD = ssc.socketTextStream("bigdata04", 9001)

    //对接收到的数据使用空格进行拆分，拆分成单词
    val wordsRDD = linesRDD.flatMap(_.split(" "))

    //把每个单词转换成tuple2的形式
    val tupRDD = wordsRDD.map((_, 1))

    //执行reduceByKey操作
    val wordcountRDD = tupRDD.reduceByKey(_ + _)

    //将结果数据打印到控制台
    wordcountRDD.print()

    //启动任务
    ssc.start()
    //等待任务停止
    ssc.awaitTermination()
  }
}
