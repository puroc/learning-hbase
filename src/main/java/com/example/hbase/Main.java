package com.example.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by puroc on 17/5/24.
 */
public class Main {

    public static final int THREAD_COUNT = 10;

    public static final long MSG_COUNT_PER_THREAD = 10000;

    private Configuration configuration;

    private String tableName = "test2";

    private String columnFamily = "abc";

    private static int columnNum = 50;

    private static String[] columns = new String[columnNum];

    private static String[] values = new String[columnNum];

    private HBaseAdmin admin;

    static {
        for (int i = 0; i < columnNum; i++) {
            columns[i] = "column-" + i;
            values[i] = "value-" + i;
        }
    }


    private void init() {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "cdh1");
            configuration.set("hbase.client.pause", "200");
            configuration.set("hbase.ipc.client.tcpnodelay", "true");

//            configuration.set("hbase.zookeeper.quorum", "master");
            admin = new HBaseAdmin(configuration);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void createTable() {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);

            if (admin.tableExists(tableName)) {
                throw new RuntimeException("table " + tableName + " has already exist.");
            }

            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(desc);
            System.out.println("create table " + tableName + " finish.");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void deleteTable() {
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " finish.");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void insert(HTable table, String row, List<Put> list) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        put.setWriteToWAL(false);
        for (int i = 0; i < columns.length; i++) {
            put.add(Bytes.toBytes(columnFamily),
                    Bytes.toBytes(String.valueOf(columns[i])),
                    Bytes.toBytes(values[i]));
        }
        list.add(put);
        if (list.size() % 100 == 0) {
            table.put(list);
            table.flushCommits();
            list.clear();
        }
    }

    class Task implements Runnable {

        private final String threadName;

        public Task(String name) {
            this.threadName = name;
        }

        public void run() {
            try {
                System.out.println(threadName + " start insert.");

                final HTable table = new HTable(configuration, tableName);
                table.setAutoFlush(false);
                table.setWriteBufferSize(24 * 1024 * 1024);

                final List<Put> list = new ArrayList<Put>();
                for (int i = 0; i < MSG_COUNT_PER_THREAD; i++) {
                    //                                String rowKey = System.currentTimeMillis() + random.nextInt
                    // (99999) + "";
                    String rowKey = threadName + "-" + i;
                    try {
                        insert(table, rowKey, list);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(threadName + " finish.");
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private void test() throws IOException {
        long start = System.currentTimeMillis();

        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Task(i + ""));
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

        System.out.println("num:" + MSG_COUNT_PER_THREAD * THREAD_COUNT + ",time:" + (stop - start));
    }

    public static void main(String[] args) throws IOException {
        final Main main = new Main();
        main.init();
//        main.deleteTable();
//        main.createTable();
        main.test();
    }
}
