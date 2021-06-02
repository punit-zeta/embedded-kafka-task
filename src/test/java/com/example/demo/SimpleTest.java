package com.example.demo;

//import com.google.common.base.Throwables;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleTest {
//    @Rule
//    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();
//
//    @Test
//    public void junitRuleShouldHaveStartedKafka() {
//        String testTopic = "TestTopic";
//
//        kafkaUnitRule.getKafkaUnit().createTopic(testTopic);
//        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "key", "value");
//
//        kafkaUnitRule.getKafkaUnit().sendMessages(keyedMessage);
//        List<String> messages = kafkaUnitRule.getKafkaUnit().readMessages(testTopic, 1);
//
//        assertEquals(Collections.singletonList("value"), messages);
//    }

//    private static final int KAFKA_PORT = 2181;
//    private static final int ZK_PORT = 9092;
//
//    private static final String SOURCE_TOPIC = "source-topic";
//    private static final String ISSUE_TOPIC = "issue-topic";
//    private static final String CRASH_TOPIC = "crash-topic";
//
//    private KafkaUnit kafkaServer;
//
//    @Before
//    public void setup() {
//
//        try {
//            kafkaServer = new KafkaUnit(ZK_PORT, KAFKA_PORT);
//            kafkaServer.startup();
//            kafkaServer.createTopic(SOURCE_TOPIC);
//            kafkaServer.createTopic(ISSUE_TOPIC);
//            kafkaServer.createTopic(CRASH_TOPIC);
//        } catch (Exception e) {
//            Throwables.propagate(e);
//        }
//
//    }
//
//    @Test
//    public void testConsumer() {
//        assertTrue(true);
//    }
//
//    @After
//    public void tearDown() {
//        if (kafkaServer != null)
//            kafkaServer.shutdown();
//    }
}
