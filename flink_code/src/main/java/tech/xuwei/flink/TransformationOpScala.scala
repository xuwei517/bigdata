package tech.xuwei.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

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
object TransformationOpScala {
  //注意：必须要添加这一行隐式转换的代码，否则下面的flatMap、Map等方法会报错
  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env = getEnv
    //map：对数据流中每个元素乘以2
    //mapOp(env)
    //flatMap：将数据流中的每行数据拆分为单词
    //flatMapOp(env)
    //filter：过滤出数据流中的偶数
    //filterOp(env)
    //keyBy：对数据流中的单词进行分组
    //keyByOp(env)
    //union：对两个数据流中的数字进行合并
    //unionOp(env)
    //connect：将两个数据流中的用户信息关联到一起
    //connectOp(env)

    //执行程序
    env.execute("TransformationOpScala")
  }

  def unionOp(env: StreamExecutionEnvironment) = {
    //第1份数据流
    val text1 = env.fromCollection(Array(1, 2, 3, 4, 5))
    //第2份数据流
    val text2 = env.fromCollection(Array(6, 7, 8, 9, 10))

    //合并流
    val unionStream = text1.union(text2)

    //使用一个线程执行打印操作
    unionStream.print().setParallelism(1)
  }


  def keyByOp(env: StreamExecutionEnvironment) = {
    //在测试阶段，可以使用fromElements构造实时数据流
    val text = env.fromElements("hello","you","hello","me")

    //处理数据
    val wordCountStream = text.map((_,1))
    val keyByStream = wordCountStream.keyBy(_._1)

    //使用一个线程执行打印操作
    keyByStream.print().setParallelism(1)
  }


  def filterOp(env: StreamExecutionEnvironment) = {
    //在测试阶段，可以使用fromElements构造实时数据流
    val text = env.fromElements(1, 2, 3, 4, 5)

    //处理数据
    val numStream = text.filter(_ % 2 == 0)

    //使用一个线程执行打印操作
    numStream.print().setParallelism(1)
  }


  def flatMapOp(env: StreamExecutionEnvironment) = {
    //在测试阶段，可以使用fromElements构造实时数据流
    val text = env.fromElements("hello you","hello me")

    //处理数据
    val wordStream = text.flatMap(_.split(" "))

    //使用一个线程执行打印操作
    wordStream.print().setParallelism(1)
  }

  def mapOp(env: StreamExecutionEnvironment) = {
    //在测试阶段，可以使用fromElements构造实时数据流
    val text = env.fromElements(1, 2, 3, 4, 5)

    //处理数据
    val numStream = text.map(_ * 2)

    //使用一个线程执行打印操作
    numStream.print().setParallelism(1)
  }

  private def getEnv = {
    StreamExecutionEnvironment.getExecutionEnvironment
  }

}
