package com.example.connect;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.storage.*;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is only a temporary extension to Kafka Connect runtime until there is an Embedded API as per KIP-26
 */

public class ConnectEmbedded {
    private static final Logger log = LoggerFactory.getLogger(ConnectEmbedded.class);
    private static final int REQUEST_TIMEOUT_MS = 120000;

    private final Worker worker;
    private final DistributedHerder herder;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final ShutdownHook shutdownHook;
    private final Properties[] connectorConfigs;

    public ConnectEmbedded(String kafkaClusterId, Properties workerConfig, Properties... connectorConfigs) throws Exception {
        Time time = Time.SYSTEM;
        //"Kafka Connect distributed worker initializing ...";
        long initStart = time.hiResClockMs();
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();

        Map<String, String> workerProps = Utils.propsToStringMap(workerConfig);

       // logger.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        DistributedConfig config = new DistributedConfig(workerProps);

        //logger.debug("Kafka cluster ID: {}", kafkaClusterId);

        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

        Object connectorClientConfigOverridePolicy = Compatibility.createConnectorClientConfigOverridePolicy(plugins, config);

       // ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = new
        worker = new Worker(workerId, time, plugins, config, offsetBackingStore, (ConnectorClientConfigOverridePolicy) connectorClientConfigOverridePolicy);
        WorkerConfigTransformer configTransformer = worker.configTransformer();

        Converter internalValueConverter = worker.getInternalValueConverter();
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time,
                internalValueConverter);
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                config,
                configTransformer);

        herder = new DistributedHerder(config, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl.toString(),
                (ConnectorClientConfigOverridePolicy) connectorClientConfigOverridePolicy);

        //logger.info("Kafka Connect distributed worker initialization took {} ms",(time.hiResClockMs() - initStart));

        this.connectorConfigs = connectorConfigs;
        shutdownHook = new ShutdownHook();
    }

    public void start() {
        try {
            log.info("Kafka ConnectEmbedded starting");
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            worker.start();
            herder.start();

            log.info("Kafka ConnectEmbedded started");

            for (Properties connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
                String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, cb);
                cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }

        } catch (InterruptedException e) {
            log.error("Starting interrupted ", e);
        } catch (ExecutionException e) {
            log.error("Submitting connector config failed", e.getCause());
        } catch (TimeoutException e) {
            log.error("Submitting connector config timed out", e);
        } finally {
            startLatch.countDown();
        }
    }

    public void stop() {
        try {
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {

                log.info("Kafka ConnectEmbedded stopping");
                herder.stop();
                worker.stop();

                log.info("Kafka ConnectEmbedded stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                startLatch.await();
                ConnectEmbedded.this.stop();
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
            }
        }
    }
}