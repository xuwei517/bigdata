package tech.xuwei.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 需求：通过Socket模拟实时产生一些单词数据，使用Flink实时接收数据，
 * 对指定时间窗口内(例如：2秒)的单词数据进行聚合统计，
 * 并且把时间窗口内计算的结果打印出来。
 *
 * Created by xuwei
 */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //连接socket获取输入数据
    val text = env.socketTextStream("bigdata04", 9001)

    //处理数据，将接收到的每一行数据根据空格拆分成单词，并且把拆分出来的单词发送出去
    //注意：必须要添加这一行隐式转换的代码，否则下面的flatMap()、Map()等方法会报错
    import org.apache.flink.api.scala._
    val wordStream = text.flatMap(_.split(" "))

    //把每一个单词转换为tuple2的形式（单词,1）
    val wordCountStream = wordStream.map((_, 1))

    //根据tuple2中的第1列进行分组
    val keyStream = wordCountStream.keyBy(_._1)

    //设置时间窗口为2 s，表示每隔2 s计算一次接收到的数据
    val windowStream = keyStream.timeWindow(Time.seconds(2))

    //根据tuple2中的第2列进行聚合
    val sumRes = windowStream.sum(1)

    //使用一个线程执行打印操作
    sumRes.print.setParallelism(1)

    //执行程序
    env.execute("SocketWindowWordCountScala")

  }
}
