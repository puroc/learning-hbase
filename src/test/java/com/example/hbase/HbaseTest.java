package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by puroc on 2016/11/5.
 */
public class HbaseTest {

    public Configuration configuration;

    public Connection con;

    @Before
    public void setUp() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "docker-1");
    }

    @Test
    public void testQueryAll() throws Exception {
        {
            Connection connection = ConnectionFactory
                    .createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("test"));
            try {
                ResultScanner rs = table.getScanner(new Scan());
                for (Result r : rs) {
                    System.out.println("获得到rowkey:" + new String(r.getRow()));
                    for (Cell cell : r.rawCells()) {
                        System.out.println("family:"
                                + Bytes.toString(cell.getFamilyArray(),
                                cell.getFamilyOffset(),
                                cell.getFamilyLength()));
                        System.out.println("Qualifier:"
                                + Bytes.toString(cell.getQualifierArray(),
                                cell.getQualifierOffset(),
                                cell.getQualifierLength()));

                        System.out.println("value:"
                                + Bytes.toString(cell.getValueArray(),
                                cell.getValueOffset(),
                                cell.getValueLength()));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //    public static void main(String[] args) {
//        try {
//            HbaseTest test = new HbaseTest();
//            test.createTable("teacher", new String[]{"name", "sex", "age"});
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//    }


//    public void createTable(String tableName, String[] columns) throws IOException {
////        Configuration conf = HBaseConfiguration.create();
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        if (admin.tableExists(tableName)) {
//            System.out.println("表已经存在！");
//        } else {
//            HTableDescriptor desc = new HTableDescriptor(tableName);
//            for (String column : columns) {
//                desc.addFamily(new HColumnDescriptor(column));
//            }
//            admin.createTable(desc);
//            System.out.println("表创建成功！");
//        }
//    }
//
//
//    public void insertRecord(String tablename, String row, String columnFamily, String[] columns, String[] values) {
//        try {
//            Configuration conf = HBaseConfiguration.create();
//            HTable table = new HTable(conf, tablename);
//            Put put = new Put(Bytes.toBytes(row));
//            for (int i = 0; i < columns.length; i++) {
//                put.add(Bytes.toBytes(columnFamily),
//                        Bytes.toBytes(String.valueOf(columns[i])),
//                        Bytes.toBytes(values[i]));
//                table.put(put);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    public void deleteRow(String tablename, String rowkey) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        HTable table = new HTable(conf, tablename);
//        List list = new ArrayList();
//        Delete d1 = new Delete(rowkey.getBytes());
//        list.add(d1);
//        table.delete(list);
//        System.out.println("删除行成功！");
//    }
//
//
//    public static void selectRow(String tablename, String rowKey)
//            throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        HTable table = new HTable(conf, tablename);
//        Get g = new Get(rowKey.getBytes());
//        Result rs = table.get(g);
//        for (KeyValue kv : rs.raw()) {
//            System.out.print(new String(kv.getRow()) + "  ");
//            System.out.print(new String(kv.getFamily()) + ":");
//            System.out.print(new String(kv.getQualifier()) + "  ");
//            System.out.print(kv.getTimestamp() + "  ");
//            System.out.println(new String(kv.getValue()));
//        }
//    }
//
//
//    public void scanAllRecord(String tablename) {
//        try {
//            Configuration conf = HBaseConfiguration.create();
//            HTable table = new HTable(conf, tablename);
//            Scan s = new Scan();
//            ResultScanner rs = table.getScanner(s);
//            for (Result r : rs) {
//                KeyValue[] kv = r.raw();
//                for (int i = 0; i < kv.length; i++) {
//                    System.out.print(new String(kv[i].getRow()) + "  ");
//                    System.out.print(new String(kv[i].getFamily()) + ":");
//                    System.out.print(new String(kv[i].getQualifier()) + "  ");
//                    System.out.print(kv[i].getTimestamp() + "  ");
//                    System.out.println(new String(kv[i].getValue()));
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    public void deleteTable(String tablename) throws IOException {
//        try {
//            Configuration conf = HBaseConfiguration.create();
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            admin.disableTable(tablename);
//            admin.deleteTable(tablename);
//            System.out.println("表删除成功！");
//        } catch (MasterNotRunningException e) {
//            e.printStackTrace();
//        } catch (ZooKeeperConnectionException e) {
//            e.printStackTrace();
//        }
//    }

}
