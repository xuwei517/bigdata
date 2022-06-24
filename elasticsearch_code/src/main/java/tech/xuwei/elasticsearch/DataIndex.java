package tech.xuwei.elasticsearch;
import tech.xuwei.utils.EsUtil;
import tech.xuwei.utils.HBaseUtil;
import tech.xuwei.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * 在Elasticsearch中对HBase中的数据建立索引
 * Created by xuwei
 */
public class DataIndex {
    private final static Logger logger = LoggerFactory.getLogger(DataIndex.class);
    public static void main(String[] args) {
        List<String> rowKeyList = null;
        Jedis jedis = null;
        try {
            //1. 首先从Redis的列表中获取Rowkey
            jedis = RedisUtil.getJedis();
            //如果brpop获取到了数据，则返回的List中有两列，第1列是key的名称，第2列是具体的数据
            rowKeyList = jedis.brpop(3, "l_article_ids");
            while (rowKeyList != null) {
                String rowKey = rowKeyList.get(1);
                //2. 根据Rowkey到HBase中获取数据的详细信息
                Map<String, String> map = HBaseUtil.getFromHBase("article", rowKey);
                //3. 在ES中对数据建立索引
                EsUtil.addIndex("article",rowKey,map);

                //循环从Redis的列表中获取Rowkey
                rowKeyList = jedis.brpop(3, "l_article_ids");
            }
        }catch (Exception e){
            logger.error("数据建立索引失败："+e.getMessage());
            //这里可以考虑把获取出来的RowKey再push到Redis中，这样可以保证数据不丢失
            if(rowKeyList!=null){
                jedis.rpush("l_article_ids",rowKeyList.get(1));
            }
        }finally {
            //向连接池返回连接
            if(jedis!=null){
                RedisUtil.returnResource(jedis);
            }
            //注意：应确认ES连接不再使用了再关闭连接，否则会导致Client无法继续使用
            try{
                EsUtil.closeRestClient();
            }catch (Exception e){
                logger.error("ES连接关闭失败："+e.getMessage());
            }
        }
    }
}
