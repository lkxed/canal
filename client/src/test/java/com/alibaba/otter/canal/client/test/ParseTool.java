package com.alibaba.otter.canal.client.test;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.alibaba.otter.canal.protocol.CanalEntry.*;

public class ParseTool {
    public static final Logger logger = LoggerFactory.getLogger(ParseTool.class);

    /**
     * 打印变更记录
     *
     * @param entries 变更记录集合
     * @throws InvalidProtocolBufferException 解析错误
     */
    public static void printEntries(List<Entry> entries) throws InvalidProtocolBufferException {
        logger.info("entries.size(): {}", entries.size());
        for (Entry entry : entries) {
            ByteString storeValue = entry.getStoreValue();
            RowChange rowChange = RowChange.parseFrom(storeValue);
            logger.info("rowChange.getEventType(): {}", rowChange.getEventType());
            List<RowData> rowDataList = rowChange.getRowDatasList();
            logger.info("rowDataList.size(): {}", rowDataList.size());
            for (RowData rowData : rowDataList) {
                List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
                List<Column> afterColumnsList = rowData.getAfterColumnsList();
                logger.info("beforeColumns: {}", parseColumns(beforeColumnsList));
                logger.info("afterColumns: {}", parseColumns(afterColumnsList));
            }
        }
    }

    public static String parseColumns(List<Column> columns) {
        StringBuilder builder = new StringBuilder();
        for (Column column : columns) {
            String name = column.getName();
            String value = column.getValue();
            builder.append(name);
            builder.append(" = ");
            builder.append(value);
            builder.append(", ");
        }
        return builder.toString();
    }
}
