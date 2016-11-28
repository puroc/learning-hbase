package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by puroc on 2016/11/5.
 */
public class HbaseTest {

    private Configuration conf;

    {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.99.129");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
//        HBASE_CONFIG.set("zookeeper.znode.parent", "/hbase");
        conf = HBaseConfiguration.create(HBASE_CONFIG);
        try {
            Connection con = ConnectionFactory.createConnection(conf);
//            con.getAdmin().createTable(new HTableDescriptor("haha"));
            System.out.println(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        try {
            HbaseTest test = new HbaseTest();
            test.createTable("teacher", new String[]{"name", "sex", "age"});
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @throws IOException
     */
    public void createTable(String tableName, String[] columns) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在！");
        } else {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (String column : columns) {
                desc.addFamily(new HColumnDescriptor(column));
            }
            admin.createTable(desc);
            System.out.println("表创建成功！");
        }
    }

    /**
     * 插入一行记录
     *
     * @param tablename    表名
     * @param row          行名称
     * @param columnFamily 列族名
     * @param columns      （列族名：column）组合成列名
     * @param values       行与列确定的值
     */
    public void insertRecord(String tablename, String row, String columnFamily, String[] columns, String[] values) {
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, tablename);
            Put put = new Put(Bytes.toBytes(row));
            for (int i = 0; i < columns.length; i++) {
                put.add(Bytes.toBytes(columnFamily),
                        Bytes.toBytes(String.valueOf(columns[i])),
                        Bytes.toBytes(values[i]));
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一行记录
     *
     * @param tablename 表名
     * @param rowkey    行名
     * @throws IOException
     */
    public void deleteRow(String tablename, String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, tablename);
        List list = new ArrayList();
        Delete d1 = new Delete(rowkey.getBytes());
        list.add(d1);
        table.delete(list);
        System.out.println("删除行成功！");
    }

    /**
     * 查找一行记录
     *
     * @param tablename 表名
     * @param rowKey    行名
     */
    public static void selectRow(String tablename, String rowKey)
            throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, tablename);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + "  ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + "  ");
            System.out.print(kv.getTimestamp() + "  ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * 查询表中所有行
     *
     * @param tablename
     */
    public void scanAllRecord(String tablename) {
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, tablename);
            Scan s = new Scan();
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                KeyValue[] kv = r.raw();
                for (int i = 0; i < kv.length; i++) {
                    System.out.print(new String(kv[i].getRow()) + "  ");
                    System.out.print(new String(kv[i].getFamily()) + ":");
                    System.out.print(new String(kv[i].getQualifier()) + "  ");
                    System.out.print(kv[i].getTimestamp() + "  ");
                    System.out.println(new String(kv[i].getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表操作
     *
     * @param tablename
     * @throws IOException
     */
    public void deleteTable(String tablename) throws IOException {
        try {
            Configuration conf = HBaseConfiguration.create();
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
            System.out.println("表删除成功！");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

}
