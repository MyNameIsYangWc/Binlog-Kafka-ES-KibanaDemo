package top.lzzly.sync.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import top.lzzly.sync.kafka.KafkaSender;
import top.lzzly.sync.model.CanalModel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: canal监听
 * @Date : 2020/6/6
 * @Author :杨文超
 */
@Component
public class CanalClientRunner implements CommandLineRunner {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    // kafka话题
    @Value("${spring.kafka.topic}")
    private String topic;

    // kafka分区
    @Value("${spring.kafka.partNum}")
    private int partNum;

    // Kafka备份数
    @Value("${spring.kafka.repeatNum}")
    private short repeatNum;

    // kafka地址
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaHost;

    @Autowired
    KafkaSender kafkaSender;

    /**
     * 创建客户端
     * @param args
     * @throws Exception
     * @Date : 2020/6/6
     * @Author :杨文超
     */
    @Async
    @Override
    public void run(String... args) throws Exception {

        //创建canal客户端
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("129.211.113.218", 11111),
                "example",//与canal配置文件保持一致
                "",
                "");

        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            logger.info("canal已连接 || 开始连接kafka");

            kafkaSender.createTopic(kafkaHost, topic, partNum, repeatNum);
            logger.info("kafka连接成功 || 开始监听canal日志");

            handleMessage(connector);
        } catch (CanalClientException e) {

            logger.error("canal客户端异常结束"+e);
            connector.rollback();
        }
    }

    /**
     * 获取canal消息体
     * @date 2020-06-06
     * @author 杨文超
     */
    private void handleMessage(CanalConnector connector){

        int batchSize = 1024;
        while (true) {
            try {

                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId != -1 || size > 0) {

                    logger.info("截取到canal数据,开始处理 || batchId:"+batchId);
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
            } catch (CanalClientException e) {
                try {
                    logger.error("canal客户端异常5s后重新连接:"+e);
                    Thread.sleep(5000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
                connector.connect();
                connector.subscribe(".*\\..*");
                connector.rollback();
                logger.info("CanalClient重连成功:"+e);
            } catch (Exception e) {
                logger.error("canal消息体异常:"+e);
                connector.rollback();
            }
        }

    }

    /**
     * 获取操作的表,事件,数据
     * @param entrys
     * @Date : 2020/6/6
     * @Author :杨文超
     */
    private void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry
                    .EntryType
                    .TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),e);
            }
            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            String schemaName = entry.getHeader().getSchemaName();//数据库名
            String tableName = entry.getHeader().getTableName();
            String event=schemaName+"."+tableName+"."+eventType;//事件
            List<CanalModel> msg=new ArrayList<>();

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if(eventType == CanalEntry.EventType.DELETE){
                    System.out.println("-------> before");
                    printColumn(event,rowData.getBeforeColumnsList(),msg);
                }else {
                    System.out.println("-------> after");
                    printColumn(event,rowData.getAfterColumnsList(),msg);
                }
                if(msg.size()==50){
                    logger.info("kafka数据达到50条发送:"+msg.size());
                    kafkaSender.send(topic,JSON.toJSONString(msg));
                    msg.clear();
                }
            }
            kafkaSender.send(topic,JSON.toJSONString(msg));
            logger.info("数据已发送kafka,条数:"+msg.size());
        }
    }

    /**
     * 处理数据发送到kafka
     * @param eventType
     * @param columns
     * @Date : 2020/6/6
     * @Author :杨文超
     */
    private void printColumn(String eventType,List<CanalEntry.Column> columns,List<CanalModel> msg) {
        Map<String, Object> map = new HashMap<>();
        CanalModel canalModel = new CanalModel(eventType, map);
        for (CanalEntry.Column column : columns) {
            map.put(column.getName(),column.getValue());
        }
        msg.add(canalModel);
    }
}
