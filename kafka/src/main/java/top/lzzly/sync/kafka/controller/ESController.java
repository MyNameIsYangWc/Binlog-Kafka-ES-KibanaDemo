package top.lzzly.sync.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import top.lzzly.sync.kafka.server.ESService;

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
     * 创建文档
     * @param index 索引 (库名)
     * @param type (表名)
     * @param object 文档(数据)
     */
    public void createDocument(String index,String type,Object object) {
        esService.createDocument(index,type,object);
    }

    /**
     * 读取文档
     * @param search 条件
     */
    public void readDocument(String search) {
        esService.readDocument(search);
    }

    /**
     * 更新文档
     * @param index 索引 (库名)
     * @param id 文档id
     * @param object 文档(数据)
     */
    public void updateDocument(String index,String id,Object object) {
        esService.updateDocument(index,id,object);
    }

    /**
     * 删除文档
     * @param index 索引 (库名)
     * @param id 文档id
     * @param object 文档(数据)
     */
    public void deleteDocument(String index,String id,Object object) {
        esService.deleteDocument(index,id,object);
    }

    /**
     * 设置索引的mapping（设置数据类型和分词方式）
     * @param index
     */
    public void createIndexMapping(String index, String type, String mappingString) {
        esService.createIndexMapping(index,type,mappingString);
    }
}
