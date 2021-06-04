package com.example.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class SampleSinkTask extends SinkTask {

    public SampleSinkTask(){
        super();
    }

    @Override
    public String version() {
        return new SampleSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> map) {
      System.out.println(map);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
       System.out.println("put called -> size: " + collection.size());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        System.out.println("flush called");
    }

    @Override
    public void stop() {

    }
}
