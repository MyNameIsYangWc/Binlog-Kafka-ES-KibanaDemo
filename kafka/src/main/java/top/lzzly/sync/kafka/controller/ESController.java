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
     * 删除索引
     * @param index
     */
    public void delteIndex(String index){
        esService.delteIndex(index);
    }

}
