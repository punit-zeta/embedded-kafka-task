package com.example.demo;

import com.example.connect.ConnectEmbedded;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@EmbeddedKafka(partitions = 1, topics = { "testTopic" })
@SpringBootTest
public class SimpleKafkaTest {

    private static final String TEST_TOPIC = "testTopic";

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;


    @Test
    public void testConnect() throws Exception {
        Producer<Integer, String> producer = configureProducer();
        producer.send(new ProducerRecord<>(TEST_TOPIC, 123, "my-test-value"));
        producer.flush();
        producer.close();
        // MockSinkConnector
        Properties workerConfig = getDefault(embeddedKafkaBroker.getBrokersAsString());;
        String kafkaClusterId = embeddedKafkaBroker.getKafkaServer(0).clusterId();
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(kafkaClusterId, workerConfig, getConnectorConfigs(TEST_TOPIC));
        connectEmbedded.start();
        Thread.sleep(2000);
    }

    private Properties getConnectorConfigs(String topics) {
        Properties props = new Properties();
        props.put(SinkConnector.TOPICS_CONFIG, topics);
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, "sample-sink-connector");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "com.example.connect.SampleSinkConnector");
        return props;
    }

    @Test
    public void testReceivingKafkaEvents() {
        Consumer<Integer, String> consumer = configureConsumer();
        Producer<Integer, String> producer = configureProducer();

        producer.send(new ProducerRecord<>(TEST_TOPIC, 123, "my-test-value"));

        ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC);
        assertNotNull(singleRecord);
        assertEquals(singleRecord.key().intValue(), 123);
        assertEquals("my-test-value", singleRecord.value());
        consumer.close();
        producer.close();
    }

    private Consumer<Integer, String> configureConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<Integer, String> consumer = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps)
                .createConsumer();
        consumer.subscribe(Collections.singleton(TEST_TOPIC));
        return consumer;
    }

    private Producer<Integer, String> configureProducer() {
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        return new DefaultKafkaProducerFactory<Integer, String>(producerProps).createProducer();
    }

    private static Properties getDefault(String broker) {
        Properties properties = new Properties();
        properties.put("group.id", "local-connect-cluster");
        properties.put("bootstrap.servers", broker);

        properties.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        properties.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        properties.put("key.converter.schemas.enable", "false");
        properties.put("value.converter.schemas.enable", "false");

        properties.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("internal.key.converter.schemas.enable", "false");
        properties.put("internal.value.converter.schemas.enable", "false");

        properties.put("offset.storage.topic", "connect-offsets");
        properties.put("offset.storage.partitions", "25");
        properties.put("offset.storage.replication.factor", "1");

        properties.put("config.storage.topic", "connect-configs");
        properties.put("config.storage.replication.factor", "1");

        properties.put("status.storage.topic", "connect-status");
        properties.put("status.storage.partitions", "5");
        properties.put("status.storage.replication.factor", "1");

        properties.put("offset.flush.interval.ms", "1000");
        properties.put("offset.flush.timeout.ms", "15000");

        properties.put("consumer.max.poll.records", "10");
        properties.put("consumer.session.timeout.ms", "20000");
        properties.put("consumer.fetch.min.bytes", "16384");

        properties.put("producer.retries", "3");
        properties.put("producer.request.timeout.ms", "1500");
        properties.put("producer.compression.type", "lz4");
        properties.put("producer.linger.ms", "5");

        return properties;
    }
}