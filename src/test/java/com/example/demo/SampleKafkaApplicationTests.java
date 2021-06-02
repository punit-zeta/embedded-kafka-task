package com.example.demo;

//import com.github.charithe.kafka.EphemeralKafkaBroker;
//import com.github.charithe.kafka.KafkaJunitRule;
//import com.google.common.collect.Lists;
//import net.jodah.concurrentunit.Waiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@SpringBootTest
class SampleKafkaApplicationTests {

//	@ClassRule
//	public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());
//
//	@Test
//	public void testSomething() throws Throwable {
//		//System.out.println(kafkaRule.helper().kafkaPort());
////		kafkaRule.helper().produceStrings("my-test-topic", "a", "b", "c", "d", "e");
////		List<String> result = kafkaRule.helper().consumeStrings("my-test-topic", 5).get();
////		System.out.println(result);
//
//		Waiter waiter = new Waiter();
//		EphemeralKafkaBroker kafkaBroker =
//				EphemeralKafkaBroker.create(8000, 8001);
//		kafkaBroker.start().get();
//
//		KafkaJunitRule kafkaRule = new KafkaJunitRule(kafkaBroker);
//		KafkaProducer testProducer = kafkaRule.helper().createStringProducer();
//		testProducer.send(
//				new ProducerRecord<>("testTopic", "testKey", "testValue")
//		);
//		testProducer.close();
//
//		Map consumerSettings = new Properties();
//		consumerSettings.put(
//				"connection_string",
//				"localhost:" + Integer.toString(kafkaRule.helper().kafkaPort())
//		);
//		consumerSettings.put("group_id", "test");
//		KafkaStreamObtainer kafkaStream =
//				new KafkaStreamObtainer(consumerSettings);
//		kafkaStream.addListener((record) -> {
//			waiter.assertEquals(record.get("key"), "testKey");
//			waiter.assertEquals(record.get("value"), "testValue");
//			waiter.resume();
//		});
//		kafkaStream.listenToStream("testTopic");
//
//		waiter.await(50000);
//	}
//
//	private static final String TOPIC = "topicY";
//
//
//	@Test
//	public void testKafkaServerIsUp() {
//		try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
//			producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
//		}
//
//		try (KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer()) {
//			consumer.subscribe(Lists.newArrayList(TOPIC));
//			ConsumerRecords<String, String> records = consumer.poll(10);
//			assertNotNull(records);
//			assertFalse(records.isEmpty());
//
//			ConsumerRecord<String, String> msg = records.iterator().next();
//			assertNotNull(msg);
//			assertEquals("keyA",msg.key());
//			assertEquals("valueA", msg.value());
//		}
//	}

}
