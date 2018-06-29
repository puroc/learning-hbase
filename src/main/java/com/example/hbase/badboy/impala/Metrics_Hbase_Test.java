package com.example.hbase.badboy.impala;

import com.example.hbase.Counter;
import com.example.hbase.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by puroc on 2017/11/16.
 */
public class Metrics_Hbase_Test {
    public static final int MIN_VALUE = 0;
    public static final int MAX_VALUE = 300;
    public static final String YEAR = "2018";
    public static final String MONTH = "05";
    public static final String DATE = "01";
    public static final String HOUR = "15";
    public static final String TABLE_NAME = "metrics_hbase_test";
    public static final String ID = "id";
    public static final String READING = "reading";
    public static final String REALVALUE = "realvalue";
    public static final String TIME = "time";

    private AtomicLong totalNum = new AtomicLong();
    private Configuration configuration;
    private Connection connection;
    private Admin admin;
    private String columnFamily = "lz";
    public static final int THREAD_COUNT = 1;
    public static final int BATCH_INSERT_NUM = 1;
    public static final long ROW_COUNT_PER_THREAD = 1;


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
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                Assert.fail();
                return;
            }
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
            System.out.println("表已删除");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void createTable() {
        Table table = null;
        try {
            if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                Assert.fail();
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
            System.out.println("表已创建");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void insertOneLine(String deviceId, HTable table, List<Put> list, String time) throws IOException, ParseException {
        Put put = new Put(Bytes.toBytes(deviceId + "-" + System.currentTimeMillis() + "-" + new Random().nextInt(9999)));
        //不写wal日志,可以提高性能
        put.setWriteToWAL(false);

        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(ID),
                Bytes.toBytes(deviceId));
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(READING),
//                Bytes.toBytes(Utils.getRandomValue(MIN_VALUE, MAX_VALUE)));
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(REALVALUE),
//                Bytes.toBytes(Utils.getRandomValue(MIN_VALUE, MAX_VALUE)));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(READING),
                Bytes.toBytes(1));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(REALVALUE),
                Bytes.toBytes(1));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(TIME),
                Bytes.toBytes(time));
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(READING),
//                Bytes.toBytes("123"));
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(REALVALUE),
//                Bytes.toBytes("456"));
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(TIME),
//                Bytes.toBytes("2018-06-16 18:00:00"));
        list.add(put);
        totalNum.incrementAndGet();
        if (list.size() % BATCH_INSERT_NUM == 0) {
            table.put(list);
            table.flushCommits();
            list.clear();
            Counter.getInstance().add();
        }
    }

    private void batchInsert(String time) throws IOException {
        Counter.getInstance().start();
        long start = System.currentTimeMillis();

        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Task("device" + i, time));
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

        System.out.println("num:" + totalNum.get() + ",time:" + (stop - start));

        Counter.getInstance().stop();
    }

    class Task implements Runnable {

        private String deviceId;
        private String time;

        private HTable table;

        public Task(String deviceId, String time) {
            this.deviceId = deviceId;
            this.time = time;
        }

        public void run() {
            try {
                System.out.println(deviceId + " start insertAllColumns.");
                table = (HTable) connection.getTable(TableName.valueOf(TABLE_NAME));
                table.setAutoFlush(false);
                table.setWriteBufferSize(24 * 1024 * 1024);
                final List<Put> list = new ArrayList<Put>();
                for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                    insertOneLine(this.deviceId, table, list, time);
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
            Metrics_Hbase_Test main = new Metrics_Hbase_Test();
            main.init();
//            main.createTable();
            String time = Utils.getDateString(YEAR, MONTH, DATE, HOUR);
            main.batchInsert(time);
//            main.deleteTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
