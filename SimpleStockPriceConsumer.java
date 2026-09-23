package com.tos.kafka.consumer.adv;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleStockPriceConsumer {

	private static Consumer<String, StockPrice> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleAdvConsumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KA" + UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
		// "KafkaExampleAdvConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		// Create the consumer using props.
		final Consumer<String, StockPrice> consumer = new KafkaConsumer<>(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(StockAppConstants.TOPIC));
		return consumer;
	} 
	
	static void runConsumer() throws InterruptedException {
		final Consumer<String, StockPrice> consumer = createConsumer();
		final List<StockPrice> map = new ArrayList<StockPrice>();
		try {
			final int giveUp = 10;
			int noRecordsCount = 0;
			//consumer.seekToBeginning(consumer.assignment());
			while (true) {
				ConsumerRecords<String, StockPrice> consumerRecords = consumer.poll(Duration.ofMillis(2000));
				if (consumerRecords.count() == 0) {
					noRecordsCount++;
					if (noRecordsCount > giveUp)
						break;
					else
						continue;
				}
				consumerRecords.forEach(record -> {
					map.add(record.value());
				});
				displayRecordsStatsAndStocks(map, consumerRecords);
				consumer.commitAsync();
			}
		} finally {
			consumer.close();
		}
		System.out.println("DONE");
	}

	private static void displayRecordsStatsAndStocks(final List<StockPrice> stockPriceMap,
			final ConsumerRecords<String, StockPrice> consumerRecords) {
		System.out.printf("New ConsumerRecords partition count: %d Message count: %d\n",
				consumerRecords.partitions().size(), consumerRecords.count());
		stockPriceMap.forEach((stockPrice) -> System.out.printf("ticker %s price %d.%d \n", stockPrice.getName(),
				stockPrice.getDollars(), stockPrice.getCents()));
		System.out.println();
	}
	
	public static void main(String[] args) {
		try {
			runConsumer();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


}
