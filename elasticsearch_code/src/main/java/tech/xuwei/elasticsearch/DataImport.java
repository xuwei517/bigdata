package tech.xuwei.elasticsearch;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import tech.xuwei.utils.HBaseUtil;
import tech.xuwei.utils.HttpUtil;
import tech.xuwei.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * 通过接口获取文章数据，入库HBase和Redis（Rowkey）
 *
 * 注意：HBase建表语句 create 'article','info'
 * Created by xuwei
 */
public class DataImport {
    private final static Logger logger = LoggerFactory.getLogger(DataImport.class);

    public static void main(String[] args) {
        //通过接口获取文章数据
        String dataUrl = "http://data.xuwei.tech/a1/wz1";
        JSONObject paramObj = new JSONObject();
        paramObj.put("num",100);     //数据条数，默认返回100条，最多支持返回1000条
        JSONObject dataObj = HttpUtil.doPost(dataUrl, paramObj);
        boolean flag = dataObj.containsKey("error");
        if(!flag){
            JSONArray resArr = dataObj.getJSONArray("data");
            for(int i=0;i<resArr.size();i++){
                JSONObject jsonObj = resArr.getJSONObject(i);
                //System.out.println(jsonObj.toJSONString());
                //文章ID作为HBase的Rowkey和ES的ID
                String id = jsonObj.getString("id");
                String title = jsonObj.getString("title");
                String author = jsonObj.getString("author");
                String describe = jsonObj.getString("describe");
                String content = jsonObj.getString("content");
                String time = jsonObj.getString("time");
                Jedis jedis = null;
                try{
                    //将数据入库HBase
                    String tableName = "article";
                    String cf = "info";
                    HBaseUtil.put2HBaseCell(tableName,id,cf,"title",title);
                    HBaseUtil.put2HBaseCell(tableName,id,cf,"author",author);
                    HBaseUtil.put2HBaseCell(tableName,id,cf,"describe",describe);
                    HBaseUtil.put2HBaseCell(tableName,id,cf,"content",content);
                    HBaseUtil.put2HBaseCell(tableName,id,cf,"time",time);
                    //将Rowkey保存到Redis中
                    jedis = RedisUtil.getJedis();
                    jedis.lpush("l_article_ids",id);
                }catch (Exception e){
                    //注意：由于HBase的PUT操作属于幂等操作，多次操作对最终的结果没有影响，所以不需要额外处理
                    logger.error("数据添加失败："+e.getMessage());
                }finally {
                    //向连接池返回连接
                    if(jedis!=null){
                        RedisUtil.returnResource(jedis);
                    }
                }

            }

        }else{
            logger.error("获取文章数据失败："+dataObj.toJSONString());
        }
    }
}
