package in.vinodsharma.reactor.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class ReactorConsumerApplication {
	public static void main(String[] args) {
		SpringApplication.run(ReactorConsumerApplication.class, args);
	}
}
