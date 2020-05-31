package top.lzzly.sync.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import top.lzzly.sync.kafka.server.ESService;

import java.util.List;

@Controller
public class ESController {

    @Autowired
    private ESService esService;

    /**
     * 创建索引
     * @param index
     */
    public void createIndex(String index){
        esService.createIndex(index);
    }

    /**
     * 检查索引是否存在
     * @param index
     */
    public void indicatesExists(String index){
        esService.indicatesExists(index);
    }

    /**
     * 删除索引
     * @param index
     */
    public void delteIndex(String index){
        esService.delteIndex(index);
    }

    /**
     * 创建文档/更新文档
     * @param index 索引 (库名)
     * @param type (表名)
     * @param entity 文档(数据)
     */
    public void operationDocument(String index,String type, List entity) {
        esService.operationDocument(index,type,entity);
    }

    /**
     * 读取文档
     * @param search 条件
     */
    public void readDocument(String search) {
        esService.readDocument(search);
    }

    /**
     * 删除文档
     * @param index 索引 (库名)
     * @param type
     * @param id  文档id
     */
    public void deleteDocument(String index,String type,List<String> id) {
        esService.deleteDocument(index,type,id);
    }

}
