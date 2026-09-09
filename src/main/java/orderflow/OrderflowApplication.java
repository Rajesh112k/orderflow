package orderflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderflowApplication.class, args);
	}

}
