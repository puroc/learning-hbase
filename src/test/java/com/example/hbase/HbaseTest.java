package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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

    private Configuration conf;

    private String tableName = "test1";

    private String[] columns = new String[]{"name", "age"};

    private String[] values = new String[]{"lisi", "18"};

    private String[] columns2 = new String[]{"sex", "career"};

    private String[] values2 = new String[]{"male", "developer"};

    private String columnFamily = "abc";

    private Admin admin;

    private Connection connection;


    @Before
    public void setUp() throws Exception {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
//            conf.set("hbase.zookeeper.quorum", "big-data-205");
            conf.set("hbase.zookeeper.quorum", "cdh1");
            //connection应全局唯一,确保所有的操作使用的都是同一个connnection
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        Table table = null;
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Assert.fail();
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDeleteTable() throws Exception {
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                Assert.fail();
                return;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
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
            insert(row1, columns, values);
            insert(row2, columns, values);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testInsert2() throws Exception {
        try {
            String row1 = "1";
            String row2 = "2";
            insert(row1, columns2, values2);
            insert(row2, columns2, values2);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void insert(String row, String[] columns, String[] values) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(row));
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                    .getColumnFamilies();
            System.out.println(columnFamilies.length);
            for (int i = 0; i < columns.length; i++) {
                put.addColumn(Bytes.toBytes(columnFamily),
                        Bytes.toBytes(String.valueOf(columns[i])),
                        Bytes.toBytes(values[i]));
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testQueryByRowKey() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("1"));
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
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testQueryByTimerange() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("1"));
            get.setTimeRange(1497501997188L, Long.MAX_VALUE);
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
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testScan() throws Exception {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
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
            if (rs != null) {
                rs.close();
            }
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testScanByRange() throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1"));
        scan.setStopRow(Bytes.toBytes("1"));
        ResultScanner rs = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
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
            if (rs != null) {
                rs.close();
            }
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testQueryByColumn() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("1"));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name")); // 获取指定列族和列修饰符对应的列
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
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testDeleteColumn() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteColumn = new Delete(Bytes.toBytes("1"));
            deleteColumn.deleteColumns(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("age"));
            table.delete(deleteColumn);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testDeleteAllColumn() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteAll = new Delete(Bytes.toBytes("2"));
            table.delete(deleteAll);
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
}
