package com.hp.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    private HBaseUtil() {

    }

    public static void makeHBaseConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    public static void insertData(String tableName,String rowKey,String family,String column,String value) throws IOException {
        Connection connection = connHolder.get();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    public static void close() throws IOException {
        Connection connection = connHolder.get();
        if (connection != null) {
            connection.close();
            connHolder.remove();
        }
    }
}
