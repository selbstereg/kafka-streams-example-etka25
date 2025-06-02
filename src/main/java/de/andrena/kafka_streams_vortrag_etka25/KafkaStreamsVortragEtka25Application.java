package de.andrena.kafka_streams_vortrag_etka25;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafka
@EnableKafkaStreams
@SpringBootApplication
public class KafkaStreamsVortragEtka25Application {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsVortragEtka25Application.class, args);
	}

}
