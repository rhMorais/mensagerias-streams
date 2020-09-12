package com.facef.kafka.categorizadorpalavras;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

@SpringBootApplication
public class CategorizadorpalavrasApplication {

	public static void main(String[] args) {
		SpringApplication.run(CategorizadorpalavrasApplication.class, args);
	}

	@Bean
	@SuppressWarnings("unchecked")
	public Function<KStream<Object, String>, KStream<Object, WordCount>[]> process() {

		Predicate<Object, WordCount> isSmall = (k, v) -> v.getKey().length() <= 5;
		Predicate<Object, WordCount> isMedium = (k, v) -> v.getKey().length() <= 10;
		Predicate<Object, WordCount> isLarge = (k, v) -> v.getKey().length() > 10;

		return input -> input.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.map((key, value) -> new KeyValue<>(value, value))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
				.windowedBy(TimeWindows.of(Duration.ofSeconds(2)))
				.count(Materialized.as("WordCounts-1"))
				.toStream()
				.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
				.branch(isSmall, isMedium, isLarge);
	}

}