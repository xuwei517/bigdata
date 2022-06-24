package tech.xuwei.spark

import org.apache.spark.{SparkConf, SparkContext}
;

/**
 * 需求：Transformation实战
 * map：对集合中每个元素乘以2
 * filter：过滤出集合中的偶数
 * flatMap：将行拆分为单词
 * groupByKey：对每个大区的主播进行分组
 * reduceByKey：统计每个大区的主播数量
 * sortByKey：对主播的音浪收入排序
 * join：打印每个主播的大区信息和音浪收入
 * distinct：统计当天开播的大区信息
 * Created by xuwei
 */
object TransformationOpScala {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext
    //map：对集合中每个元素乘以2
    //mapOp(sc)
    //filter：过滤出集合中的偶数
    //filterOp(sc)
    //flatMap：将行拆分为单词
    //flatMapOp(sc)
    //groupByKey：对每个大区的主播进行分组
    //groupByKeyOp(sc)
    //groupByKeyOp2(sc)
    //reduceByKey：统计每个大区的主播数量
    //reduceByKeyOp(sc)
    //sortByKey：对主播的金币收入进行排序
    //sortByKeyOp(sc)
    //join：打印每个主播的大区信息和金币收入
    //joinOp(sc)
    //distinct：统计当天开播的大区信息
    //distinctOp(sc)
    sc.stop()
  }

  def distinctOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array((150001,"US"),(150002,"CN"), (150003,"CN"),(150004,"IN")))
    //由于是统计开播的大区信息，所以需要根据大区信息去重
    dataRDD.map(_._2).distinct().foreach(println(_))
  }



  def joinOp(sc: SparkContext): Unit = {
    val dataRDD1 = sc.parallelize(Array((150001,"US"),(150002,"CN"), (150003,"CN"),(150004,"IN")))
    val dataRDD2 = sc.parallelize(Array((150001,400),(150002,200), (150003,300),(150004,100)))

    val joinRDD = dataRDD1.join(dataRDD2)
    //joinRDD.foreach(println(_))

    joinRDD.foreach(tup=>{
      //用户ID
      val uid = tup._1
      val area_gold = tup._2
      //大区
      val area = area_gold._1
      //金币收入
      val gold = area_gold._2
      println(uid+"\t"+area+"\t"+gold)
    })
  }

  def sortByKeyOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array((150001,400),(150002,200),(150003,300),(150004,100)))
    //由于需要对金币收入进行排序，所以需要把金币收入作为key，这里要进行位置互换
    /*dataRDD.map(tup=>(tup._2,tup._1))
      .sortByKey(false)	//默认是正序，第1个参数为true，如果要采用倒序则需要把这个参数设置为false
      .foreach(println(_))*/

    //sortBy的使用：可以动态指定排序的字段，比较灵活
    dataRDD.sortBy(_._2,false).foreach(println(_))

  }

  def reduceByKeyOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array((150001,"US"),(150002,"CN"), (150003,"CN"),(150004,"IN")))
    //由于这个需求只需要使用到大区信息，所以在map操作时只保留大区信息即可
    //为了计算大区的数量，所以在大区后面拼上了1，组装成了tuple2这种形式，这样就可以使用reduceByKey了
    dataRDD.map(tup=>(tup._2,1)).reduceByKey(_ + _).foreach(println(_))
  }

  def groupByKeyOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array((150001,"US"),(150002,"CN"), (150003,"CN"),(150004,"IN")))
    //需要使用map对tuple中的数据进行位置互换，因为我们需要把大区作为key进行分组操作
    //此时的key就是tuple中的第1列，其实在这里可以把这个tuple认为是一个key-value
    //注意：在使用类似于groupByKey这种基于key的算子时，需要提前把RDD中的数据组装成tuple2这种形式
    //此时map算子之后生成的新的数据格式是这样的：("US",150001)
    //如果tuple中的数据列数超过了2列怎么办？看groupByKeyOp2
    dataRDD.map(tup=>(tup._2,tup._1)).groupByKey().foreach(tup=>{
      //获取大区信息
      val area = tup._1
      print(area+":")
      //获取同一个大区对应的所有用户ID
      val it = tup._2
      for(uid <- it){
        print(uid+" ")
      }
      println()
    })
  }

  def groupByKeyOp2(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array((150001,"US","male"),(150002,"CN", "female"),(150003,"CN","male"),(150004,"IN","female")))
    //如果tuple中的数据列数超过了2列怎么办？
    //把需要作为key的那一列作为tuple2的第1列，剩下的可以再使用一个tuple2包装一下
    //此时map算子之后生成的新的数据格式是这样的：("US",(150001,"male"))
    //注意：如果你的数据结构比较复杂，则可以在执行每一个算子之后都调用foreach打印一下，确认数据的格式
    dataRDD.map(tup=>(tup._2,(tup._1,tup._3))).groupByKey().foreach(tup=>{
      //获取大区信息
      val area = tup._1
      print(area+":")
      //获取同一个大区对应的所有用户ID和性别信息
      val it = tup._2
      for((uid,sex) <- it){
        print("<"+uid+","+sex+"> ")
      }
      println()
    })
  }

  def flatMapOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array("good good study","day day up"))
    dataRDD.flatMap(_.split(" ")).foreach(println(_))
  }

  def filterOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    dataRDD.filter(_ % 2 ==0).foreach(println(_))
  }

  def mapOp(sc: SparkContext): Unit ={
    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    dataRDD.map(_ * 2).foreach(println(_))
  }

  /**
   * 获取SparkContext
   * @return
   */
  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("TransformationOpScala")
      .setMaster("local")
    new SparkContext(conf)
  }
}
