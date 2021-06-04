package com.example.connect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class SampleSinkTask extends SinkTask {

    SampleSinkTask(){
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
    public void open(Collection<TopicPartition> partitions) {
        System.out.println(partitions.size());
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
       System.out.println(collection.size());
    }

    @Override
    public void stop() {

    }
}
