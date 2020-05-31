package top.lzzly.sync.kafka.common.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.lzzly.sync.kafka.entity.Role;
import top.lzzly.sync.kafka.entity.User;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Description:实例类映射
 * @Date : 2020/05/31
 * @Author : 杨文超
 */
public class OrmEntityUtil {

    private static Logger logger= LoggerFactory.getLogger(OrmEntityUtil.class);

    /**
     * 根据表明映射实体类并补充数据
     * @param tableName
     * todo 增加实体类,此方法需要增加对应的代码映射
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    public static JSONObject ormEntity(String tableName, JSONArray message){

        if(!tableName.isEmpty() && message !=null && message.size()>0){
            if("User".equalsIgnoreCase(tableName)){//用户表
                logger.warn("补充User表数据");
                //为实体类赋值
                JSONObject entity = getEntityKey(new User(), message);
                return entity;

            }else if("Role".equalsIgnoreCase(tableName)){//角色表
                logger.warn("补充Role表数据");
                //为实体类赋值
                JSONObject entity = getEntityKey(new Role(), message);
                return entity;

            }
        }

        return null;
    }

    /**
     * 根据类反射获取私有属性key,并赋值
     * @param className
     * @param message kafka接收到的数据
     * @return
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    private static JSONObject getEntityKey(Object className,JSONArray message){
        JSONObject entity=new JSONObject();
        List<String> entityKey = new ArrayList<>();

        //获取实体类的私有属性key
        if(className != null){
            Class<?> aClass = className.getClass();
            Field[] fields = aClass.getDeclaredFields();
            List<Field> fields1 = Arrays.asList(fields);
            fields1.forEach(item->{
                entityKey.add(item.getName());
            });
        }
        //为实体类私有属性赋值
        if(entityKey !=null && entityKey.size()>0){
            entity = getEntityKey(entityKey, message);
        }

        return entity;
    }

    /**
     * 根据key为实体类赋值
     * @param entityKey
     * @param message kafka接收到的数据
     * @return
     * @Date : 2020/05/31
     * @Author : 杨文超
     */
    private static JSONObject getEntityKey(List<String> entityKey,JSONArray message){
        JSONObject entity=new JSONObject();
        if(entityKey !=null && entityKey.size()>0){
            try {
                //循环为key赋值
                for (int i = 0; i < entityKey.size(); i++) {
                    //key的顺序与entity保持一致并与message保持一致（实体类的key顺序与表的key顺序必须保持一致）
                    entity.put(entityKey.get(i),message.get(i));
                }
            } catch (IndexOutOfBoundsException e) {
                logger.error("根据key为实体类赋值异常：实体类key数量与kafka接受message数量不一致");
            }
        }
        return entity;
    }

}
