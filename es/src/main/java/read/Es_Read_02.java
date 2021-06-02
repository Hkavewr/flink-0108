package read;

import com.sun.xml.internal.bind.v2.TODO;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Es_Read_02 {
    public static void main(String[] args) throws IOException {
        //1.创建连接工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.读数据
        //TODO 相当于查询语句最外成{}
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //TODO bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //TODO term
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "女");

        //TODO filter
        boolQueryBuilder.filter(termQueryBuilder);

        //TODO must
//        TermQueryBuilder termQueryBuilder1 = new TermQueryBuilder("favo", "球");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "羽毛球");
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO query
        sourceBuilder.query(boolQueryBuilder);

        //TODO 聚合组 aggs
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id");
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");
        sourceBuilder.aggregation(aggregationBuilder.subAggregation(maxAggregationBuilder));

        //TODO from
        sourceBuilder.from(0);
        //TODO size
        sourceBuilder.size(2);

        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();
        //5.执行查询语句
        SearchResult result = jestClient.execute(search);

        //5.1获取命中数据条数
        Long total = result.getTotal();
        System.out.println("命中"+total+"条数据");

        //5.2获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        //5.2.3遍历存放明细数据的list集合，获取每一条明细数据
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //a.获取索引名
            String index = hit.index;
            System.out.println(index);

            //b.获取类型名
            String type = hit.type;
            System.out.println(type);

            //c.获取文档id
            String id = hit.id;
            System.out.println(id);

            //d.从source获取具体数据
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }

        //5.3获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        //5.3.1获取按照班级分组的聚合组数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            //获取key
            String key = bucket.getKey();
            System.out.println("key:"+key);
            //获取相同分组下数据个数
            Long count = bucket.getCount();
            System.out.println("doc_count:"+count);

            //获取嵌套聚合组数据（同班级下年龄最大的）
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            Double maxAge = groupByAge.getMax();
            System.out.println("value:"+maxAge);
        }

        //关闭连接
        jestClient.shutdownClient();
    }
}
