package com.tos.kafka.producer.adv;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class SimpleStockProducer {
	public final static String TOPIC = "stock-prices";

	public static Properties initProducer() {
		// Assign topicName to string variable

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "kafka0:9092");
		// props.put("bootstrap.servers", "localhost:9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");
		props.put("enable.auto.commit", "true");
		// If the request fails, the producer can automatically retry,
		props.put("retries", 1);

		// Specify buffer size in config
		props.put("batch.size", 1);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 100);

		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put("buffer.memory", 302);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", StockPriceSerializer.class.getName());
		// props.put("transactional.id", "my-transactional-id");
		return props;

	}


	public static void invokeSync () {
		Properties props = initProducer();
		Producer<String, StockPrice> producer = new KafkaProducer<String, StockPrice>(props);
		sendMessage(producer, TOPIC);
	}

	public static void invokeAsync () {
		Properties props = initProducer();
		Producer<String, StockPrice> producer = new KafkaProducer<String, StockPrice>(props);
		
		sendMessageAsync(producer, TOPIC);

	}

	 public static void sendMessage(Producer<String, StockPrice> producer, String topic) {

			for (int i = 0; i < 2; i++) {
				try {
					 ProducerRecord <String, StockPrice> record =  createRandomRecord(i);
					 RecordMetadata future = producer.send(record).get();
					 System.out.println(">>>" + future.topic() + " Offset:" + future.offset());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				producer.flush();
				System.out.println("Message sent successfully");
			}
			producer.close();
			// test(producer);
			System.out.println("Message sent successfully & Close");
		}

	 private static ProducerRecord<String, StockPrice> createRandomRecord(int i) {
			final int dollarAmount = i* 200;
			final int centAmount = i * 1000;
			final StockPrice stockPrice = new StockPrice(" STOCK"+ i, dollarAmount, centAmount);
			return new ProducerRecord<>(TOPIC, stockPrice.getName(), stockPrice);
		}

	// Sending Message Asynchronously
		public static void sendMessageAsync(Producer<String, StockPrice> producer, String topic) {
			final CountDownLatch countDownLatch = new CountDownLatch(2);	
			for (int i = 0; i < 4; i++) {
				try {
					 ProducerRecord <String, StockPrice> record =  createRandomAsyncRecord(i);
					 
					    long time = System.currentTimeMillis();
					        // Register a call back.
							 producer.send(record, (metadata, exception) -> {
					                long elapsedTime = System.currentTimeMillis() - time;
					                if (metadata != null) {
					                    System.out.printf("sent record(key=%s value=%s) " +
					                                    "meta(partition=%d, offset=%d) time=%d\n",
					                            record.key(), record.value(), metadata.partition(),
					                            metadata.offset(), elapsedTime);
					                } else {
					                    exception.printStackTrace();
					                }
					                countDownLatch.countDown();
					            });
		 
						        countDownLatch.await(25, TimeUnit.SECONDS);
							 
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				producer.flush();
				System.out.println("Message sent successfully");
			}
			producer.close();
			// test(producer);
			System.out.println("Message sent successfully & Close");
		}
		private static ProducerRecord<String, StockPrice> createRandomAsyncRecord(int i) {
			final int dollarAmount = i* 200;
			final int centAmount = i * 1000;
			final StockPrice stockPrice = new StockPrice(" Async STOCK "+ i, dollarAmount, centAmount);
			return new ProducerRecord<>(TOPIC, stockPrice.getName(), stockPrice);
		}
		
		public static void main(String[] args) {
			
	     invokeSync();
		 //invokeAsync();

}



}
