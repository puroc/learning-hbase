package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by puroc on 2016/11/5.
 */
public class HbaseTest {

    private Configuration configuration;

    private String tableName = "test2";

    private String[] columns = new String[]{"name", "age"};

    private String columnFamily = "abc";

    private String[] values = new String[]{"lisi", "18"};

    private HBaseAdmin admin;


    @Before
    public void setUp() throws Exception {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "cdh1");
            admin = new HBaseAdmin(configuration);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            admin.close();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(configuration);

        if (admin.tableExists(tableName)) {
            Assert.fail();
        }

        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(desc);
    }

    @Test
    public void testDeleteTable() throws Exception {
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testInsert() throws Exception {
        try {
            String row1 = "1";
            String row2 = "2";
            insert(row1);
            insert(row2);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    private void insert(String row) throws IOException {
        HTable table = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes(row));

        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();
        System.out.println(columnFamilies.length);
        for (int i = 0; i < columns.length; i++) {
            put.add(Bytes.toBytes(columnFamily),
                    Bytes.toBytes(String.valueOf(columns[i])),
                    Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    @Test
    public void testQueryByRowKey() throws Exception {
        try {
            Get get = new Get(Bytes.toBytes("1"));
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));// 获取表
            Result result = table.get(get);
            for (KeyValue kv : result.list()) {
                System.out.println("family:" + Bytes.toString(kv.getFamily()));
                System.out
                        .println("qualifier:" + Bytes.toString(kv.getQualifier()));
                System.out.println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("Timestamp:" + kv.getTimestamp());
                System.out.println("-------------------------------------------");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testScan() throws Exception {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            rs.close();
        }

    }

    @Test
    public void testScanByRange() throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1"));
        scan.setStopRow(Bytes.toBytes("1"));
        ResultScanner rs = null;
        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            rs.close();
        }

    }

    @Test
    public void testQueryByColumn() throws Exception {
        try {
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));
            Get get = new Get(Bytes.toBytes("1"));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age")); // 获取指定列族和列修饰符对应的列
            Result result = table.get(get);
            for (KeyValue kv : result.list()) {
                System.out.println("family:" + Bytes.toString(kv.getFamily()));
                System.out
                        .println("qualifier:" + Bytes.toString(kv.getQualifier()));
                System.out.println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("Timestamp:" + kv.getTimestamp());
                System.out.println("-------------------------------------------");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDeleteColumn() throws Exception {
        try {
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));
            Delete deleteColumn = new Delete(Bytes.toBytes("1"));
            deleteColumn.deleteColumns(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("age"));
            table.delete(deleteColumn);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDeleteAllColumn() throws Exception {
        try {
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));
            Delete deleteAll = new Delete(Bytes.toBytes("2"));
            table.delete(deleteAll);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
