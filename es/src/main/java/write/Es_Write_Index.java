package write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es_Write_Index {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工程
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置参数，来连接ES
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.写入数据
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"102\",\n" +
                "  \"movie_name\":\"速度与激情9\"\n" +
                "}")
                .index("movie0108")
                .type("_doc")
                .id("1002")
                .build();

        jestClient.execute(index);


        //关闭连接
        jestClient.shutdownClient();
    }
}
