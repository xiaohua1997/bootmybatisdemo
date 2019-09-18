package com.moumou.bootmybatisdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;

@SpringBootApplication(exclude = {JmxAutoConfiguration.class})
public class BootmybatisdemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(BootmybatisdemoApplication.class, args);
	}

}
