package com.webhook_reliability;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class WebhookReliabilityApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebhookReliabilityApplication.class, args);
	}

}
