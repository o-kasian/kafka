/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests MM2 replication of newly created topics from source to target.
 * <p>
 * Validates that:
 * - topics are created during the first start
 * - new topics in source will trigger new topics creation in target
 * - newly created topics are assigned to source tasks
 */
@Category(IntegrationTest.class)
public class MirrorTopicCreationIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(MirrorTopicCreationIntegrationTest.class);

    private ExecutorService executor;
    private MirrorMakerConfig mm2Config;
    private EmbeddedConnectCluster primary;
    private EmbeddedConnectCluster backup;

    @Before
    public void setup() throws IOException {
        executor = Executors.newSingleThreadExecutor();

        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");

        Map<String, String> mm2Props = new HashMap<>();
        mm2Props.put("clusters", "primary, backup");
        mm2Props.put("replication.factor", "1");
        mm2Props.put("max.tasks", "2");
        mm2Props.put("topics", ".*test-topic-.*");
        mm2Props.put("groups", "consumer-group-.*");
        mm2Props.put("primary->backup.enabled", "true");
        mm2Props.put("sync.topic.acls.enabled", "false");
        mm2Props.put("refresh.topics.interval.seconds", "1");
        mm2Props.put("refresh.groups.interval.seconds", "1");

        mm2Config = new MirrorMakerConfig(mm2Props);
        Map<String, String> primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget("backup", "primary"));
        Map<String, String> backupWorkerProps = mm2Config.workerConfig(new SourceAndTarget("primary", "backup"));

        primary = new EmbeddedConnectCluster.Builder()
                .name("primary-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(primaryWorkerProps)
                .build();

        backup = new EmbeddedConnectCluster.Builder()
                .name("backup-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(brokerProps)
                .workerProps(backupWorkerProps)
                .build();

        primary.start();
        backup.start();

        primary.kafka().createTopic("test-topic-1", 1);


        log.info("primary REST service: {}", primary.endpointForResource("connectors"));
        log.info("backup REST service: {}", backup.endpointForResource("connectors"));

        // now that the brokers are running, we can finish setting up the Connectors
        mm2Props.put("primary.bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put("backup.bootstrap.servers", backup.kafka().bootstrapServers());
        mm2Config = new MirrorMakerConfig(mm2Props);

        backup.configureConnector("MirrorSourceConnector", mm2Config.connectorBaseConfig(new SourceAndTarget("primary", "backup"),
                MirrorSourceConnector.class));
    }

    @After
    public void close() throws IOException, InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        for (String x : primary.connectors()) {
            primary.deleteConnector(x);
        }
        for (String x : backup.connectors()) {
            backup.deleteConnector(x);
        }
        primary.stop();
        backup.stop();
    }

    @Test
    public void testNewTopicsCreation() throws InterruptedException {
        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig("primary"));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig("backup"));

        assertFalse("Started in a non-empty cluster", primaryClient.listTopics().contains("test-topic-new"));
        assertFalse("Started in a non-empty cluster", backupClient.listTopics().contains("primary.test-topic-new"));

        //verify that initial set of topics has been created
        waitFor(15, () -> backupClient.listTopics().contains("primary.test-topic-1"));

        primary.kafka().createTopic("test-topic-new", 1);

        //verify that new topic is replicated to backup
        waitFor(15, () -> backupClient.listTopics().contains("primary.test-topic-new"));

        //send some messages to source topic, check replication status
        primary.kafka().produce("test-topic-new", "test-key", "test-value");

        Consumer<byte[], byte[]> consumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", "consumer-group-1"), "primary.test-topic-new");
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
        consumer.commitSync();
        consumer.close();

        assertEquals("Messages were not replicated from source", 1, records.count());
        final Iterator<ConsumerRecord<byte[], byte[]>> iterator = records
                .records("primary.test-topic-new").iterator();
        assertTrue(iterator.hasNext());
    }

    private void waitFor(int timeoutSec, Callable<Boolean> condition) {
        long currentTime = System.currentTimeMillis();
        long waitUntil = currentTime + TimeUnit.SECONDS.toMillis(timeoutSec);
        long pollIntervalMs = TimeUnit.SECONDS.toMillis(1);
        while (System.currentTimeMillis() < waitUntil) {
            try {
                long startTime = System.currentTimeMillis();
                if (executor.submit(condition).get(pollIntervalMs, TimeUnit.MILLISECONDS)) {
                    return;
                }
                long timeToWait = pollIntervalMs - (System.currentTimeMillis() - startTime);
                if (timeToWait > 0) {
                    Thread.sleep(timeToWait);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Wait for condition failed");
    }
}
