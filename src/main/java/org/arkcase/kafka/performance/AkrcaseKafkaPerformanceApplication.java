package main.java.org.arkcase.kafka.performance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AkrcaseKafkaPerformanceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AkrcaseKafkaPerformanceApplication.class, args);
    }

}
