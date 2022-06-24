package tech.xuwei.storm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 对订单数据进行清洗
 *
 * Created by xuwei
 */
public class DataCleanBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        //获取原始订单数据
        String order_data = input.getString(0);
        //解析原始订单数据中的核心字段
        JSONObject jsonObject = JSON.parseObject(order_data);
        String order_id = jsonObject.getString("order_id");
        int price = jsonObject.getIntValue("price");
        //根据需求组装结果
        String res = order_id+","+price;
        //将结果发送给下一个组件
        outputCollector.emit(new Values(res));
        //向Spout组件确认已成功处理订单数据
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //注意：如果后面使用KafkaBolt组件接收数据，则此处的字段名称必须是message
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
