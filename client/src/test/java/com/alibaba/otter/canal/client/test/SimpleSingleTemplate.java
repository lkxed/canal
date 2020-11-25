package com.alibaba.otter.canal.client.test;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

import static com.alibaba.otter.canal.client.test.ParseTool.printEntries;

public class SimpleSingleTemplate {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("vm1", 11111),
                "cap1",
                "canal",
                "canal"
        );
        connector.connect();
        connector.subscribe(".*\\..*");
        Message message = null;
        try {
            while (true) {
                message = connector.getWithoutAck(100);
                /* do something with message ... */
                printEntries(message.getEntries());
                connector.ack(message.getId()); // connector.rollback(message.getId())
            }
        } catch (InvalidProtocolBufferException e) {
            connector.rollback(message.getId());
            e.printStackTrace();
        } finally {
            // connector.unsubscribe();
            connector.disconnect();
        }
    }
}
