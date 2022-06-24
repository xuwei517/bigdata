package tech.xuwei.storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

/**
 * 实时清洗订单数据（实时ETL）
 *
 * Created by xuwei
 */
public class DataProcessTopology {

    public static void main(String[] args) throws Exception{
        //将消费到的Kafka数据转换为Storm中的Tuple
        ByTopicRecordTranslator<String,String> brt =
                new ByTopicRecordTranslator<String,String>( (r) -> new Values(r.value(),r.topic()),new Fields("values","topic"));
        //配置KafkaSpout
        KafkaSpoutConfig<String,String> ksc = KafkaSpoutConfig
                //指定Kafka集群机制和Kafka的输入Topic
                .builder("bigdata01:9092,bigdata02:9092,bigdata03:9092", "order_data")
                //设置group.id
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "g001")
                //设置消费的起始位置
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                //设置提交消费Offset的时长间隔
                .setOffsetCommitPeriodMs(10_000)
                //配置Translator
                .setRecordTranslator(brt)
                .build();

        //配置KafkaBolt
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        KafkaBolt kafkaBolt = new KafkaBolt()
                .withProducerProperties(props)
                //指定输出目的地Topic
                .withTopicSelector(new DefaultTopicSelector("order_data_clean"));

        //组装Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaspout", new KafkaSpout<String,String>(ksc));
        builder.setBolt("dataclean_bolt", new DataCleanBolt()).shuffleGrouping("kafkaspout");
        builder.setBolt("kafkaBolt",kafkaBolt).shuffleGrouping("dataclean_bolt");


        //提交Topology
        Config config = new Config();
        String topologyName = DataProcessTopology.class.getSimpleName();
        StormTopology stormTopology = builder.createTopology();
        if(args.length==0){
            //创建本地Storm集群
            LocalCluster cluster = new LocalCluster();
            //向本地Storm集群提交Topology
            cluster.submitTopology(topologyName, config, stormTopology);
        }else{
            //向生产环境的Storm集群提交Topology
            StormSubmitter.submitTopology(topologyName,config,stormTopology);
        }
    }
}
