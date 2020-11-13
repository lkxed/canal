package com.alibaba.otter.canal.client.test;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SimpleSingleTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleSingleTest.class);

    public static void main(String[] args) {
        String host = "vm1";
        int port = 11111;
        SocketAddress address = new InetSocketAddress(host, port);
        String username = "canal";
        String password = "canal";
        String destination = "cap1";

        CanalConnector connector = CanalConnectors
                .newSingleConnector(address, destination, username, password);

        logger.info("connecting to {}:{}...", host, port);
        connector.connect();
        logger.info("connected to {}:{}", host, port);

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
                    List<CanalEntry.Entry> entries = message.getEntries();
                    logger.info("message size: {}", batchSize);
                    ParseTool.printEntries(entries);
                }
                connector.ack(batchId);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }
}
