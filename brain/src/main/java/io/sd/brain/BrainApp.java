package io.sd.brain;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BrainApp {
	public static void main(String[] args) {
		SpringApplication.run(BrainApp.class, args);
	}
}