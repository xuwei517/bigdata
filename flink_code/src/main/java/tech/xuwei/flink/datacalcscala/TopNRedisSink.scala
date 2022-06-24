package tech.xuwei.flink.datacalcscala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.Jedis

/**
 * Created by xuwei
 */
class TopNRedisSink extends RichSinkFunction[Tuple2[String,Long]] {
  var host: String = _
  var port: Int = _
  var key: String = _

  var jedis: Jedis = _
  /**
   * 构造函数
   * @param host
   * @param port
   * @param key
   */
  def this(host: String,port: Int,key: String){
    this()
    this.host = host
    this.port = port
    this.key = key
  }

  /**
   * 初始化方法，只执行一次
   * 适合初始化资源链接
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    this.jedis = new Jedis(host,port)
  }

  /**
   * 核心代码，来一条数据此方法会执行一次
   *
   * @param value
   * @param context
   */
  override def invoke(value: (String, Long), context: SinkFunction.Context[_]): Unit = {
    jedis.zincrby(key,value._2,value._1)
  }

  /**
   * 任务停止时会先调用此方法
   * 适合关闭资源链接
   */
  override def close(): Unit = {
    //关闭链接
    if(jedis!=null){
      jedis.close()
    }
  }

}
