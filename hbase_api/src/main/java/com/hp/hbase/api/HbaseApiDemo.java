package com.hp.hbase.api;

import com.hp.coprocessor.InsertStudentCoprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class HbaseApiDemo {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        try {
            admin.getNamespaceDescriptor("ASD");
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor asd = NamespaceDescriptor.create("ASD").build();
            admin.createNamespace(asd);
        }

        if (!admin.tableExists(TableName.valueOf("ASD:student"))) {
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf("ASD:student"));
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info1")).build();
            ColumnFamilyDescriptor info2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info2")).build();
            tdb.setColumnFamilies(Arrays.asList(columnFamilyDescriptor, info2));
            TableDescriptor build = tdb.build();
            admin.createTable(build);
        }
        Table table = connection.getTable(TableName.valueOf("ASD:student"));

        Get get = new Get(Bytes.toBytes("1001"));

        Result result = table.get(get);
        if (result.isEmpty()) {
            Put put = new Put(Bytes.toBytes("1001"));
            put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("age"), Bytes.toBytes(12));
            put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
            table.put(put);

        } else {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            }
//            Delete delete = new Delete(Bytes.toBytes("1001"), 1574692995632L);
//            table.delete(delete);
        }

        Table student = connection.getTable(TableName.valueOf("student"));

        Scan scan = new Scan();
        ResultScanner scanner = student.getScanner(scan);
        for (Result result1 : scanner) {
            for (Cell cell : result1.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            }
        }


        table.close();
        connection.close();
    }
}
