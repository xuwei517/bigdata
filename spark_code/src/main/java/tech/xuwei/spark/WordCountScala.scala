package tech.xuwei.spark

import org.apache.spark.{SparkConf, SparkContext}
;

/**
 * 需求：单词计数
 * Created by xuwei
 */
object WordCountScala {

  def main(args: Array[String]): Unit = {
    //第一步：创建SparkContext
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")	//设置任务名称
      .setMaster("local")//local表示在本地执行
    val sc = new SparkContext(conf)

    //第二步：加载数据
    var path = "hdfs://bigdata01:9000/hello.txt"
    if(args.length==1){
      path = args(0)
    }
    val linesRDD = sc.textFile(path)

    //第三步：对数据进行拆分，把一行数据拆分成多个单词
    val wordsRDD = linesRDD.flatMap(_.split(" "))

    //第四步：迭代word，将每个word转换为(word,1)这种形式
    val pairRDD = wordsRDD.map((_,1))

    //第五步：根据key（其实就是word）进行分组聚合统计
    val wordCountRDD = pairRDD.reduceByKey(_ + _)

    //第六步：将结果打印到控制台上
    //注意：只有当任务执行到这一行代码时，任务才会真正开始执行计算
    //如果任务中没有这一行代码，则前面的所有算子是不会执行的
    wordCountRDD.foreach(wordCount=>println(wordCount._1+"\t"+wordCount._2))

    //第七步：停止SparkContext
    sc.stop()
  }

}
