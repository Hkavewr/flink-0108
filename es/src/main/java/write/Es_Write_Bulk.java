package write;

import bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es_Write_Bulk {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据
        Movie movie3 = new Movie("103", "人之怒");
        Movie movie4 = new Movie("104", "死神来了");
        Movie movie5 = new Movie("105", "迪迦");

        Index index3 = new Index.Builder(movie3).id("1003").build();
        Index index4 = new Index.Builder(movie4).id("1004").build();
        Index index5 = new Index.Builder(movie5).id("1005").build();
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie0108")
                .defaultType("_doc")
                .addAction(index3)
                .addAction(index4)
                .addAction(index5)
                .build();
        jestClient.execute(bulk);

        //关闭连接
        jestClient.shutdownClient();
    }
}
