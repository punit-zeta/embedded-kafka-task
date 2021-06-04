package com.example.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSinkConnector extends SinkConnector {
    private Map<String, String> config;
    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    public SampleSinkConnector() {
        super();
    }

    @Override
    public void start(Map<String, String> map) {
        config = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SampleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(config);

        for (int i = 0; i < maxTasks; i++)
            taskConfigs.add(taskProps);

        return taskConfigs;
    }

    @Override
    public void stop() {
       // do nothing
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
