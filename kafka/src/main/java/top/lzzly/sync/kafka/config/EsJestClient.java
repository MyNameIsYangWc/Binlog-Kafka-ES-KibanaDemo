package top.lzzly.sync.kafka.config;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.util.Objects;

/**
 * @Description: Jest客户端
 * @Date : 2020/5/30
 * @Author : 杨文超
 */
public class EsJestClient {

    private static JestClient client;

    /**
     * 获取客户端
     * @return jestclient
     * @Date : 2020/5/30
     * @Author : 杨文超
     */
    public static synchronized JestClient getClient() {
        if (client == null) {
            build();
        }
        return client;
    }

    /**
     * 关闭客户端
     * @Date : 2020/5/30
     * @Author : 杨文超
     */
    public static void close(JestClient client) {
        if (!Objects.isNull(client)) {
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 建立连接
     * @Date : 2020/5/30
     * @Author : 杨文超
     */
    private static void build() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig
                        .Builder(Config.ES_HOST)
                        .multiThreaded(true)
                        //一个route 默认不超过2个连接  路由是指连接到某个远程注解的个数。总连接数=route个数 * defaultMaxTotalConnectionPerRoute
                        .defaultMaxTotalConnectionPerRoute(2)
                        //所有route连接总数
                        .maxTotalConnection(2)
                        .connTimeout(10000)
                        .readTimeout(10000)
                        .gson(new GsonBuilder()
                                .setDateFormat("yyyy-MM-dd HH:mm:ss")
                                .create())
                        .build()
        );
        client = factory.getObject();
    }

}
