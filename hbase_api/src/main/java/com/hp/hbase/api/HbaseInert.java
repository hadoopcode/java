package com.hp.hbase.api;

import com.hp.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseInert {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor("Test");
        } catch (NamespaceNotFoundException e) {
            e.printStackTrace();
            NamespaceDescriptor test = NamespaceDescriptor.create("Test").build();
            admin.createNamespace(test);
        }

        TableName tableName = TableName.valueOf("Test:test1");
        if (!admin.tableExists(tableName)) {
            ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(info).build();
            admin.createTable(tableDescriptor);
        }

        Table table = connection.getTable(tableName);

        List<Put> list = new ArrayList<Put>();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes(Integer.toString(i)));
            String name = Integer.toString(i) + (char) (Math.random() * 26 + 'A');
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(Integer.toString(i+10)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(Integer.toString(i)));
            list.add(put);
        }
        table.put(list);

        admin.close();
        table.close();
        connection.close();
    }
}
