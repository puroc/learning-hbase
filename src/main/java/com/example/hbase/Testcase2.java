package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by puroc on 2017/11/16.
 */
public class Testcase2 {

    private AtomicLong rowkey = new AtomicLong();
    private Configuration configuration;
    private Connection connection;
    private Admin admin;
    private String tableName = "IOT_WATER_QUALITY_HISTORY";
    private String columnFamily = "lz";
    private static int columnNum = 20;
    private static String[] columns = new String[columnNum];
    private static String[] values = new String[columnNum];
    public static final String COLUMN = "column-";
    public static final String VALUE = new String(new byte[32]);
    public static final int THREAD_COUNT = 2;
    public static final int BATCH_INSERT_NUM = 100;
    public static final long ROW_COUNT_PER_THREAD = 1000;

    static {
        for (int i = 0; i < columnNum; i++) {
            columns[i] = COLUMN + i;
            values[i] = VALUE;
        }
    }


    private void init() {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "cdh0");
            configuration.set("hbase.client.pause", "200");
            configuration.set("hbase.ipc.client.tcpnodelay", "true");
            configuration.set("hbase.client.write.buffer", "100");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void deleteTable() {
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                Assert.fail();
                return;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表已删除");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void createTable() {
        Table table = null;
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Assert.fail();
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
            System.out.println("表已创建");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void insertOneLine(String deviceId, HTable table, String row, List<Put> list) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        //不写wal日志,可以提高性能
        put.setWriteToWAL(false);

        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes("Id"),
                Bytes.toBytes(deviceId));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes("ph"),
                Bytes.toBytes("" + Utils.getRandomValue(6,8)));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes("ntu"),
                Bytes.toBytes("" + Utils.getRandomValue(0,10)/10.0));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes("clclo2"),
                Bytes.toBytes("" + Utils.getRandomValue(1,8)/10.0));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes("time"),
                Bytes.toBytes(""+System.currentTimeMillis()));
        for (int i = 0; i < columns.length; i++) {
            put.add(Bytes.toBytes(columnFamily),
                    Bytes.toBytes(columns[i]),
                    Bytes.toBytes(values[i]));
        }
        list.add(put);
        if (list.size() % BATCH_INSERT_NUM == 0) {
            table.put(list);
            table.flushCommits();
            list.clear();
            Counter.getInstance().add();
        }
    }

    private void batchInsert() throws IOException {
        Counter.getInstance().start();
        long start = System.currentTimeMillis();

        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Task("device-"+i ));
            threadList.add(thread);
        }

        //启动线程
        for (Thread thread : threadList) {
            thread.start();
        }

        //等待线程结束
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        long stop = System.currentTimeMillis();

        System.out.println("num:" + ROW_COUNT_PER_THREAD + ",time:" + (stop - start));

        Counter.getInstance().stop();
    }

    class Task implements Runnable {

        private String deviceId;

        private HTable table;

        public Task(String deviceId) {
            this.deviceId = deviceId;
        }

        public void run() {
            try {
                System.out.println(deviceId + " start insertAllColumns.");
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlush(false);
                table.setWriteBufferSize(24 * 1024 * 1024);
                final List<Put> list = new ArrayList<Put>();
                for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                    String key = rowkey.incrementAndGet() + "";
                    insertOneLine(this.deviceId,table, key, list);
                }
                System.out.println(deviceId + " finish.");
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Testcase2 main = new Testcase2();
            main.init();
//            main.createTable();
            main.batchInsert();
//            main.deleteTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
