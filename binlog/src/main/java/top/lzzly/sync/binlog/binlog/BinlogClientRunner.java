package top.lzzly.sync.binlog.binlog;

import com.alibaba.fastjson.JSON;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import top.lzzly.sync.binlog.dto.BinlogDto;
import top.lzzly.sync.binlog.kafka.KafkaSender;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author :
 * @Description: binlog监听
 * @Date : 2019/3/20  10:37
 * @Modified By :
 */
@Component
public class BinlogClientRunner implements CommandLineRunner {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    @Value("${binlog.host}")
    private String host;

    @Value("${binlog.port}")
    private int port;

    @Value("${binlog.user}")
    private String user;

    @Value("${binlog.password}")
    private String password;

    // binlog server_id
    @Value("${server.id}")
    private long serverId;

    // kafka话题
    @Value("${kafka.topic}")
    private String topic;

    // kafka分区
    @Value("${kafka.partNum}")
    private int partNum;

    // Kafka备份数
    @Value("${kafka.repeatNum}")
    private short repeatNum;

    // kafka地址
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaHost;

    // 指定监听的数据表
    @Value("${binlog.database.table}")
    private String database_table;

    @Autowired
    KafkaSender kafkaSender;

    @Async
    @Override
    public void run(String... args) throws Exception {

        // 创建topic
        kafkaSender.createTopic(kafkaHost, topic, partNum, repeatNum);
        // 获取监听数据表数组
        List<String> databaseList = Arrays.asList(database_table.split(","));
        HashMap<Long, String> tableMap = new HashMap<Long, String>();
        // 创建binlog监听客户端
        BinaryLogClient client = new BinaryLogClient(host, port, user, password);
        client.setServerId(serverId);
        logger.warn("监听中。。。");
        client.registerEventListener((event -> {
            // binlog事件
            EventData data = event.getData();
            if (data != null) {
                logger.info("data###:"+data);
                if (data instanceof TableMapEventData) {
                    TableMapEventData tableMapEventData = (TableMapEventData) data;
                    logger.warn("###触发的表###"+tableMapEventData.getTable());
                    tableMap.put(tableMapEventData.getTableId(), tableMapEventData.getDatabase() + "." + tableMapEventData.getTable());
                }
                // update数据
                if (data instanceof UpdateRowsEventData) {
                    logger.warn("触发更新。。。");
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) data;
                    String tableName = tableMap.get(updateRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        String eventKey = tableName + ".update";
                        for (Map.Entry<Serializable[], Serializable[]> row : updateRowsEventData.getRows()) {
                            String msg = JSON.toJSONString(new BinlogDto(eventKey, row.getValue()));
                            kafkaSender.send(topic, msg);
                        }
                    }
                }
                // insert数据
                else if (data instanceof WriteRowsEventData) {
                    logger.warn("触发新增。。。");
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) data;
                    String tableName = tableMap.get(writeRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        String eventKey = tableName + ".insert";
                        for (Serializable[] row : writeRowsEventData.getRows()) {
                            String msg = JSON.toJSONString(new BinlogDto(eventKey, row));
                            kafkaSender.send(topic, msg);
                        }
                    }
                }
                // delete数据
                else if (data instanceof DeleteRowsEventData) {
                    logger.warn("触发删除。。。");
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) data;
                    String tableName = tableMap.get(deleteRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        String eventKey = tableName + ".delete";
                        for (Serializable[] row : deleteRowsEventData.getRows()) {
                            String msg = JSON.toJSONString(new BinlogDto(eventKey, row));
                            kafkaSender.send(topic, msg);
                        }
                    }
                }
            }
        }));
        client.connect();
    }
}
