package tech.xuwei.flink.datacalcjava;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 1:统计全站双11当天实时GMV
 * 2:统计实时销量TopN的商品品类
 * Created by xuwei
 */
public class StreamDataCalcJava {
    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定FlinkKafkaConsumer相关配置
        String topic = "order_detail";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092");
        prop.setProperty("group.id","con");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        //指定Kafka作为Source
        DataStreamSource<String> text = env.addSource(kafkaConsumer);

        //解析订单数据，只保留需要用到的字段
        //goodsCount、goodsPrice、goodsType
        SingleOutputStreamOperator<Tuple3<Long, Long, String>> orderStream = text.flatMap(new FlatMapFunction<String, Tuple3<Long, Long, String>>() {
            public void flatMap(String line, Collector<Tuple3<Long, Long, String>> out) throws Exception {
                JSONObject orderJson = JSON.parseObject(line);
                //获取JSON数据中的商品明细
                JSONArray orderDetail = orderJson.getJSONArray("detal");
                for (int i = 0; i < orderDetail.size(); i++) {
                    JSONObject orderObj = orderDetail.getJSONObject(i);
                    long goodsCount = orderObj.getLongValue("goodsCount");
                    long goodsPrice = orderObj.getLongValue("goodsPrice");
                    String goodsType = orderObj.getString("goodsType");
                    out.collect(new Tuple3<Long, Long, String>(goodsCount, goodsPrice, goodsType));
                }
            }
        });

        //过滤异常数据
        SingleOutputStreamOperator<Tuple3<Long, Long, String>> filterStream = orderStream.filter(new FilterFunction<Tuple3<Long, Long, String>>() {
            public boolean filter(Tuple3<Long, Long, String> tup) throws Exception {
                //商品数量大于0的数据才是有效数据
                return tup.f0 > 0;
            }
        });

        //1.统计全站“双十一”当天的实时GMV
        SingleOutputStreamOperator<Long> gmvStream = filterStream.map(new MapFunction<Tuple3<Long, Long, String>, Long>() {
            public Long map(Tuple3<Long, Long, String> tup) throws Exception {
                //计算单个商品的消费金额
                return tup.f0 * tup.f1;
            }
        });
        //将GMV数据保存到Redis中
        gmvStream.addSink(new GmvRedisSink("bigdata04",6380,"gmv"));

        //2.统计实时销量Top N的商品品类
        SingleOutputStreamOperator<Tuple2<String, Long>> topNStream = filterStream.map(new MapFunction<Tuple3<Long, Long, String>, Tuple2<String, Long>>() {
            //获取商品品类和购买的商品数量
            public Tuple2<String, Long> map(Tuple3<Long, Long, String> tup) throws Exception {
                return new Tuple2<String, Long>(tup.f2, tup.f0);
            }
        });
        //根据商品品类分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyStream = topNStream.keyBy(0);
        //设置时间窗口为1 s
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowStream = keyStream.timeWindow(Time.seconds(1));
        //求和，指定tuple中的第2列，即商品数量
        SingleOutputStreamOperator<Tuple2<String, Long>> resStream = windowStream.sum(1);
        //将goods_type数据保存到Redis中
        resStream.addSink(new TopNRedisSink("bigdata04",6380,"goods_type"));

        //执行任务
        env.execute("StreamDataCalcJava");
    }
}
