package tech.xuwei.elasticsearch;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;

/**
 * Search详解
 * Created by xuwei
 */
public class EsSearchOp {
    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01", 9200, "http"),
                        new HttpHost("bigdata02", 9200, "http"),
                        new HttpHost("bigdata03", 9200, "http")));


        SearchRequest searchRequest = new SearchRequest();
        //指定索引库，支持指定一个或者多个，也支持通配符，例如：user*
        searchRequest.indices("user");


        //指定查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询所有，可以不指定，默认是查询索引库中的所有数据
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //对指定字段的值进行过滤，注意：在查询数据时会对数据进行分词
        //如果指定了多个query，则后面的query会覆盖前面的query
        //对字符串类型内容的查询，不支持通配符
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","tom"));
        //searchSourceBuilder.query(QueryBuilders.matchQuery("age","17")); //对于age的值，这里可以指定字符串或者数字都可以
        //对于字符串类型内容的查询，支持通配符，但是性能较差，可以认为是全表扫描
        //searchSourceBuilder.query(QueryBuilders.wildcardQuery("name","t*"));
        //区间查询，主要针对数据类型，可以使用from+to 或者gt、gte+lt、lte
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).to(20));
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").gte(0).lte(20));
        //不限制边界，指定为null即可
        //searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).to(null));
        //同时指定多个条件，条件之间的关系支持and(must)、or(should)
        //searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name","tom")).should(QueryBuilders.matchQuery("age",19)));
        //在多条件组合查询时，可以设置条件的权重值，将满足高权重值条件的数据排到结果列表的前面
        //searchSourceBuilder.query(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name","tom").boost(1.0f)).should(QueryBuilders.matchQuery("age",19).boost(5.0f)));
        //对多个指定字段的值进行过滤，注意：多个字段的数据类型必须一致，否则会报错，如果查询的字段不存在则不会报错
        //searchSourceBuilder.query(QueryBuilders.multiMatchQuery("tom","name","tag"));
        //这里通过queryStringQuery可以支持Lucene的原生查询语法，更加灵活，注意：AND、OR、TO之类的关键字必须大写
        //searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:tom AND age:[15 TO 30]"));
        //searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name","tom")).must(QueryBuilders.rangeQuery("age").from(15).to(30)));
        //queryStringQuery支持通配符，但是性能也是比较差
        //searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:t*"));
        //精确查询，查询时不分词，在针对人名、手机号、主机名、邮箱号码等字段的查询时一般不需要分词
        //初始化一条测试数据name=刘德华，默认情况下在建立索引时“刘德华”会被切分为刘、德、华这3个词
        //所以这里精确查询是查不出来的，使用matchQuery是可以查出来的
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","刘德华"));
        //searchSourceBuilder.query(QueryBuilders.termQuery("name","刘德华"));
        //正常情况下，“通过termQuery实现精确查询的字段”是不能进行分词的
        //但是有时会遇到某个字段已经进行了分词，但还想要实现精确查询
        //重新建立索引也无法现实了，怎么办呢？
        //可以借助queryStringQuery来解决此问题
        //searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:\"刘德华\""));
        //matchQuery默认会根据分词的结果进行 OR 操作，满足任意一个词语的数据都会被查询出来
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","刘德华"));
        //如果要对matchQuery的分词结果实现AND操作，则可以通过operator进行设置
        //这种方式也可以解决某个字段已经分词建立索引了，后期还想要实现精确查询的问题（间接实现，其实是查询了满足刘、德、华这3个词的内容）
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name","刘德华").operator(Operator.AND));

        //分页
        //设置每页的起始位置，默认是0
        searchSourceBuilder.from(0);
        //设置每页的数据量，默认是10
        searchSourceBuilder.size(10);

        //排序
        //按照age字段倒序排序
        searchSourceBuilder.sort("age", SortOrder.DESC);
        //注意：age字段是数字类型，不需要分词；name字段是字符串类型（Text），默认会被分词，所以不支持排序和聚合操作
        //如果想根据这些会被分词的字段进行排序或者聚合，则需要指定使用它们的keyword类型，这个类型表示不会对数据分词
        searchSourceBuilder.sort("name.keyword", SortOrder.DESC);
        //keyword类型的特性其实也适用于精确查询的场景，可以在matchQuery中指定字段的keyword类型来实现精确查询，不管在建立索引时有没有被分词都不影响使用
        searchSourceBuilder.query(QueryBuilders.matchQuery("name.keyword", "刘德华"));

        //高亮
        //设置高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("name");     //支持多个高亮字段，使用多个field()方法指定即可
        //设置高亮字段的前缀和后缀内容
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取查询返回的结果
        SearchHits hits = searchResponse.getHits();
        //获取数据总数
        long numHits = hits.getTotalHits().value;
        System.out.println("数据总数："+numHits);
        //获取具体内容
        SearchHit[] searchHits = hits.getHits();
        //迭代解析具体内容
        for (SearchHit hit : searchHits) {
            /*String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);*/
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            int age = Integer.parseInt(sourceAsMap.get("age").toString());
            //获取高亮字段的内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //获取name字段的高亮内容
            HighlightField highlightField = highlightFields.get("name");
            if(highlightField!=null){
                Text[] fragments = highlightField.getFragments();
                name = "";
                for (Text text : fragments) {
                    name += text;
                }
            }
            //获取最终的结果数据
            System.out.println(name+"---"+age);
        }

        //关闭连接
        client.close();

    }
}
