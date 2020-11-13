package com.alibaba.otter.canal.client.test;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import static com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZkClusterTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleSingleTest.class);
    public static void main(String[] args) {
        String zkServers = "vm1:2181,vm1:2182,vm1:2183";
        String username = "canal";
        String password = "canal";
        String destination = "cap1";

        CanalConnector connector = CanalConnectors
                .newClusterConnector(zkServers, destination, username, password);

        logger.info("connecting to {}...", zkServers);
        connector.connect();
        logger.info("connected to {}", zkServers);

        String filter = ".*\\..*";
        connector.subscribe(filter);
        logger.info("subscribed {} from {}", filter, destination);

        int batchSize = 10;
        int timeCount = 0;
        try {
            while (true) {
                logger.info("getting message... {}s", timeCount++);
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1L || size == 0) {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    logger.info("message id: {}", batchId);
                    List<Entry> entries = message.getEntries();
                    logger.info("message size: {}", batchSize);
                    ParseTool.printEntries(entries);
                }
                connector.ack(batchId);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        finally {
            connector.disconnect();
        }
    }
}
