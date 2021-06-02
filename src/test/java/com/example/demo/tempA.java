package com.example.demo;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class tempA {
    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void test() throws Exception {
//        ZooKeeperEmbedded zooKeeperEmbedded = new ZooKeeperEmbedded();
//        System.out.println(zooKeeperEmbedded.connectString());
//        Properties properties = new Properties();
//        properties.put("zookeeper.connect", zooKeeperEmbedded.connectString());
//        KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(properties);
//        kafkaEmbedded.createTopic("abc");


    }
    @Test
    public void testSomething() throws ExecutionException, InterruptedException {
        // Convenience methods to produce and consume messages
        kafkaRule.helper().produceStrings("TOPIC", "a", "b", "c", "d", "e");
        List<String> result = kafkaRule.helper().consumeStrings("TOPIC", 5).get();

        // or use the built-in producers and consumers
        KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer();

        KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer();

        // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
        String zkConnStr = kafkaRule.helper().zookeeperConnectionString();
        int brokerPort = kafkaRule.helper().kafkaPort();
    }

}
