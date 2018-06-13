package com.example.phoenix.badboy;

import com.example.hbase.Counter;
import com.example.hbase.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.junit.Assert;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by puroc on 2017/11/16.
 */
public class PUD_TEST_WATER_HISTORY {
    public static final int MIN_VALUE = 1100;
    public static final int MAX_VALUE = 1200;
    public static final String YEAR = "2020";
    public static final String MONTH = "01";
    public static final String DATE = "01";
    public static final String HOUR = "15";
    public static final String TABLE_NAME = "PUD_TEST_WATER_HISTORY";
    public static final String INSERT_SQL = "upsert into " + TABLE_NAME + " values(?,?,?,?,?)";
    public static final String DELETE_SQL = "delete from " + TABLE_NAME;

//phoenix建表语句
//    create table IOT_WATER_QUALITY_HISTORY ("ROW" varchar primary key, "lz"."Id" varchar, "lz"."ph" varchar,"lz"."ntu" varchar,"lz"."clclo2" varchar,"lz"."time" varchar);

    private AtomicLong totalNum = new AtomicLong();
    private Configuration configuration;
    private Connection connection;
    private Admin admin;

    private String columnFamily = "lz";

    public static final int THREAD_COUNT = 50;
    public static final long ROW_COUNT_PER_THREAD = 100;

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

    private void insert(String deviceId, PhoenixConnection conn, String time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        PreparedStatement stmt = null;
        try {
            int upsertBatchSize = conn.getMutateBatchSize();
            stmt = conn.prepareStatement(INSERT_SQL);
            int rowCount = 0;
            for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                String rowKey = deviceId + "-" + System.currentTimeMillis() + "-" + new Random().nextInt(9999);
                stmt.setString(1, rowKey);
                stmt.setString(2, deviceId);
                int randomValue = Utils.getRandomValue(MIN_VALUE, MAX_VALUE);
                if (i / 10 == 0) {
                    randomValue = -1;
                }
                stmt.setInt(3, randomValue);
                stmt.setInt(4, randomValue);
                stmt.setTimestamp(5, new Timestamp(format.parse(time).getTime()));
                stmt.execute();
                totalNum.incrementAndGet();
                // Commit when batch size is reached
                if (++rowCount % upsertBatchSize == 0) {
                    conn.commit();
                    System.out.println("Rows upserted: " + rowCount);
                }
            }
            conn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
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

        public Task(String deviceId, String time) {
            this.deviceId = deviceId;
            this.time = time;
        }

        public void run() {
            PhoenixConnection conn = null;
            try {
                System.out.println(deviceId + " start insertAllColumns.");
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:cdh0");
                conn.setAutoCommit(false);
                insert(this.deviceId, conn, time);
                System.out.println(deviceId + " finish.");
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }

    public void cleanData() {
        PhoenixConnection conn = null;
        PreparedStatement stmt = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:cdh0");
            stmt = conn.prepareStatement(DELETE_SQL);
            stmt.execute();
            conn.commit();
            System.out.println("数据已清除");
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            PUD_TEST_WATER_HISTORY main = new PUD_TEST_WATER_HISTORY();
            main.init();
//            main.createTable();
            String time = Utils.getDateString(YEAR, MONTH, DATE, HOUR);
            main.batchInsert(time);
//            main.cleanData();
//            main.deleteTable();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
