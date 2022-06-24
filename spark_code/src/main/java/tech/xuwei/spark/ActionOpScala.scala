package tech.xuwei.spark

import org.apache.spark.{SparkConf, SparkContext}
;

/**
 * 需求：Action实战
 * reduce：聚合计算
 * collect：获取元素集合
 * take(n)：获取前n个元素
 * count：获取元素总数
 * saveAsTextFile：保存文件
 * countByKey：统计相同的key出现多少次
 * foreach：迭代遍历元素
 *
 * Created by xuwei
 */
object ActionOpScala {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext
    //reduce：聚合计算
    //reduceOp(sc)
    //collect：获取元素集合
    //collectOp(sc)
    //take(n)：获取前n个元素
    //takeOp(sc)
    //count：获取元素总数
    //countOp(sc)
    //saveAsTextFile：保存文件
    //saveAsTextFileOp(sc)
    //countByKey：统计相同的key出现了多少次
    //countByKeyOp(sc)
    //foreach：迭代遍历元素
    //foreachOp(sc)

    sc.stop()
  }

  def foreachOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    //注意：foreach算子中不仅限于执行println操作，在这里执行println操作只是为了测试
    //在实际工作中，如果需要把计算的结果保存到第三方的存储介质中，则需要使用foreach
    //在foreach内部实现具体向外部输出数据的代码
    dataRDD.foreach(println(_))
  }

  def countByKeyOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(("A",1001),("B",1002),("A",1003),("C",1004)))
    //返回的是一个map类型的数据
    val res = dataRDD.countByKey()
    for((k,v) <- res){
      println(k+","+v)
    }
  }


  def saveAsTextFileOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    //指定HDFS的路径信息，需要指定一个不存在的目录
    dataRDD.saveAsTextFile("hdfs://bigdata01:9000/out0524")
  }


  def countOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    val res = dataRDD.count()
    println(res)
  }

  def takeOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    //从RDD中获取前2个元素
    val res = dataRDD.take(2)
    for(item <- res){
      println(item)
    }
  }

  def collectOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    //collect返回的是一个Array数组
    //注意：如果RDD中的数据量过大，则不建议使用collect，因为最终的数据会返回给Driver进程所在的节点
    //如果要获取几条数据，则查看一下数据格式，可以使用take(n)
    val res = dataRDD.collect()
    for(item <- res){
      println(item)
    }
  }

  def reduceOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    val num = dataRDD.reduce(_ + _)
    println(num)
  }

  /**
   * 获取SparkContext
   * @return
   */
  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("ActionOpScala")
      .setMaster("local")
    new SparkContext(conf)
  }
}
