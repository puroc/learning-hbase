package com.example.hbase.badboy;

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by puroc on 2017/11/16.
 */
public class PUD_TEST_WATER_HISTORY {
    public static final int MIN_VALUE = 0;
    public static final int MAX_VALUE = 300;
    public static final String YEAR = "2018";
    public static final String MONTH = "06";
    public static final String DATE = "01";
    public static final String HOUR = "15";
    public static final String TABLE_NAME = "PUD_TEST_2";
//    public static final String TABLE_NAME = "PUD_TEST_WATER_HISTORY";
    public static final String ID = "ID";
    public static final String READING = "READING";
    public static final String REALVALUE = "REALVALUE";
    public static final String TIME = "TIME";

//phoenix建表语句
//create table PUD_TEST_2 ("ROW" varchar primary key, "lz"."ID" varchar, "lz"."READING" unsigned_int,"lz"."REALVALUE" unsigned_int,"lz"."TIME" unsigned_time);

    private AtomicLong totalNum = new AtomicLong();
    private Configuration configuration;
    private Connection connection;
    private Admin admin;

    private String columnFamily = "lz";
//    private static int columnNum = 20;
//    private static String[] columns = new String[columnNum];
//    private static String[] values = new String[columnNum];
//    public static final String COLUMN = "column-";
//    public static final String VALUE = new String(new byte[32]);
    public static final int THREAD_COUNT = 50;
    public static final int BATCH_INSERT_NUM = 100;
    public static final long ROW_COUNT_PER_THREAD = 5000;

//    static {
//        for (int i = 0; i < columnNum; i++) {
//            columns[i] = COLUMN + i;
//            values[i] = VALUE;
//        }
//    }


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
        Put put = new Put(Bytes.toBytes(deviceId+"-"+System.currentTimeMillis()+"-"+new Random().nextInt(9999)));
        //不写wal日志,可以提高性能
        put.setWriteToWAL(false);

        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(ID),
                Bytes.toBytes(deviceId));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(READING),
                Bytes.toBytes( Utils.getRandomValue(MIN_VALUE,MAX_VALUE)));
//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(REALVALUE),
//                Bytes.toBytes("" + Utils.getRandomValue(MIN_VALUE, MAX_VALUE)));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(REALVALUE),
                Bytes.toBytes(1));

        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//        put.add(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(TIME),
//                Bytes.toBytes(""+format.parse(time).getTime()));
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(TIME),
                Bytes.toBytes(format.parse(time).getTime()));

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
            Thread thread = new Thread(new Task("device"+i,time ));
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

        public Task(String deviceId,String time) {
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
                    insertOneLine(this.deviceId,table, list,time);
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
            PUD_TEST_WATER_HISTORY main = new PUD_TEST_WATER_HISTORY();
            main.init();
            main.createTable();
            String time = Utils.getDateString(YEAR, MONTH, DATE, HOUR);
            main.batchInsert(time);
//            main.deleteTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
