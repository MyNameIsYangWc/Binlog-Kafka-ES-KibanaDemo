package top.lzzly.sync.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaMiddlewareApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaMiddlewareApplication.class, args);
		System.out.println("KafkaMiddlewareApplication启动成功！！！");
	}

}
