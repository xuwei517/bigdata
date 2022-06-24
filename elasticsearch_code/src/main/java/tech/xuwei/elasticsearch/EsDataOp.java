package tech.xuwei.elasticsearch;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 针对Elasticsearch中索引数据的操作
 * 增删改查
 * Created by xuwei
 */
public class EsDataOp {
    private static Logger logger = LogManager.getLogger(EsDataOp.class);

    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01", 9200, "http"),
                        new HttpHost("bigdata02", 9200, "http"),
                        new HttpHost("bigdata03", 9200, "http")));

        //添加索引
        //addIndexByJson(client);
        //addIndexByMap(client);

        //查询索引
        //getIndex(client);
        //getIndexByFiled(client);

        //更新索引
        //注意：可以使用创建索引直接完整更新已存在的数据
        //updateIndexByPart(client);      //局部更新

        //删除索引
        //deleteIndex(client);

        //关闭连接
        client.close();
    }

    private static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteRequest request = new DeleteRequest("emp", "10");
        //执行
        client.delete(request, RequestOptions.DEFAULT);
    }

    private static void updateIndexByPart(RestHighLevelClient client) throws IOException {
        UpdateRequest request = new UpdateRequest("emp", "10");
        String jsonString = "{\"age\":23}";
        request.doc(jsonString, XContentType.JSON);
        //执行
        client.update(request, RequestOptions.DEFAULT);
    }

    private static void getIndexByFiled(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest("emp", "10");
        //只查询部分字段
        String[] includes = new String[]{"name"};        //指定包含哪些字段
        String[] excludes = Strings.EMPTY_ARRAY;         //指定过滤哪些字段
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String index = response.getIndex();
        String id = response.getId();
        if(response.isExists()){//如果没有查询到文档数据，则isExists返回false
            //获取JSON字符串格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);
            //获取MAP格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        }else{
            logger.warn("没有查询到索引库{}中id为{}的文档!",index,id);
        }
    }

    private static void getIndex(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest("emp", "10");
        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String index = response.getIndex();
        String id = response.getId();
        if(response.isExists()){	    //如果没有查询到文档数据，则isExists返回false
            //获取JSON字符串格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);
            //获取MAP格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        }else{
            logger.warn("没有查询到索引库{}中id为{}的文档!",index,id);
        }
    }

    private static void addIndexByMap(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("11");
        HashMap<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("name", "tom");
        jsonMap.put("age", 17);
        request.source(jsonMap);
        //执行
        client.index(request, RequestOptions.DEFAULT);
    }

    private static void addIndexByJson(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("emp");
        request.id("10");
        String jsonString = "{" +
                "\"name\":\"jessic\"," +
                "\"age\":20" +
                "}";
        request.source(jsonString, XContentType.JSON);
        //执行
        client.index(request, RequestOptions.DEFAULT);
    }
}
