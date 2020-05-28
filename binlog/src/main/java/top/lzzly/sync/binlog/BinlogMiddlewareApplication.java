package top.lzzly.sync.binlog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class BinlogMiddlewareApplication {
    public static void main(String[] args) {
        SpringApplication.run(BinlogMiddlewareApplication.class, args);
        System.out.println("BinlogMiddlewareApplication启动成功！！！");
    }
}
