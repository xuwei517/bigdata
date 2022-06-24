package tech.xuwei.flink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 需求：通过Socket模拟实时产生一些单词数据，使用Flink实时接收数据，
 * 对指定时间窗口内(例如：2秒)的单词数据进行聚合统计，
 * 并且把时间窗口内计算的结果打印出来。
 *
 * Created by xuwei
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception{
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("bigdata04", 9001);

        SingleOutputStreamOperator<String> wordStream = text.flatMap(new FlatMapFunction<String, String>() {
            //处理数据，将接收到的每一行数据根据空格拆分成单词
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    //把拆分出来的单词发送出去
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            //把每一个单词转换为tuple2的形式  (单词,1)
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //根据tuple2中的第1列进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyStream = wordCountStream.keyBy(0);


        //设置时间窗口为2 s，表示每隔2 s计算一次接收到的数据
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowStream = keyStream.timeWindow(Time.seconds(2));

        //根据tuple2中的第2列进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumRes = windowStream.sum(1);

        //使用一个线程执行打印操作
        sumRes.print().setParallelism(1);

        //执行程序
        env.execute("SocketWindowWordCountJava");
    }

}
