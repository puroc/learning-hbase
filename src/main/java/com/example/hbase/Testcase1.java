package com.example.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by puroc on 17/5/24.
 */
public class Testcase1 {

    //    执行插入操作的线程数
    public static final int THREAD_COUNT = 5;

    //    每个插入线程插入的记录数
    public static final long ROW_COUNT_PER_THREAD = 10000;

    //    一次批量插入操作的消息数
    public static final int BATCH_INSERT_NUM = 100;

    private Configuration configuration;

    //    表名
    private String tableName = "test2";

    private String columnFamily = "abc";

    //    每条记录的字段数量
    private static int columnNum = 17280;

    //    查询操作要查询的字段数量
    public static final int QUERY_NUM = 1440;

    private static String[] columns = new String[columnNum];

    private static String[] values = new String[columnNum];

    private HBaseAdmin admin;

    private Connection connection;

    public static final String COLUMN = "column-";

    public static final String VALUE = "value-";

    static {
        for (int i = 0; i < columnNum; i++) {
            columns[i] = COLUMN + i;
            values[i] = VALUE + i;
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

    private void deleteTable() {
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

    private void insertAllColumns(HTable table, String row, List<Put> list) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        put.setWriteToWAL(false);
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
        }
    }

    private void insertOneColumn(HTable table, String row, int index, List<Put> list) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        put.setWriteToWAL(false);
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columns[index]),
                Bytes.toBytes(values[index]));
        list.add(put);
        if (list.size() % BATCH_INSERT_NUM == 0) {
            table.put(list);
            table.flushCommits();
            list.clear();
        }
    }


    class Task implements Runnable {

        private String threadName;

        private HTable table;

        private boolean insertByRow;

        public Task(String name, boolean insertByRow) {
            this.threadName = name;
            this.insertByRow = insertByRow;
        }

        public void run() {

            try {
                System.out.println(threadName + " start insertAllColumns.");

                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlush(false);
                table.setWriteBufferSize(24 * 1024 * 1024);

                final List<Put> list = new ArrayList<Put>();

                if (insertByRow) {
                    insertByRow(list);
                } else {
                    insertByColumn(list);
                }

                System.out.println(threadName + " finish.");
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

        private void insertByRow(List<Put> list) {
            for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                String rowKey = threadName + "-" + i;
                if (i % BATCH_INSERT_NUM == 0) {
                    System.out.println("thread " + threadName + " insert " + i + " row");
                }
                try {
                    insertAllColumns(table, rowKey, list);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        private void insertByColumn(List<Put> list) {
            for (int j = 0; j < columnNum; j++) {
                for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                    try {
                        String rowKey = threadName + "-" + i;
                        if (j % BATCH_INSERT_NUM == 0 || i == columnNum) {
                            System.out.println("thread " + threadName + " insert " + j + " Column");
                        }
                        insertOneColumn(table, rowKey, j, list);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void batchInsert(boolean insertByRow) throws IOException {
        long start = System.currentTimeMillis();

        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Task(i + "", insertByRow));
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

        System.out.println("num:" + ROW_COUNT_PER_THREAD * THREAD_COUNT + ",time:" + (stop - start));
    }

    private void batchQuery(boolean byTimeRange) {
        long start = System.currentTimeMillis();
        try {
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));

            //查询第一行数据
            Get get = new Get(Bytes.toBytes("0-1"));
            if (byTimeRange) {
                get.setTimeRange(get.getTimeRange().getMin(), get.getTimeRange().getMax());
            } else {
                for (int i = 0; i < QUERY_NUM; i++) {
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(COLUMN + i)); // 获取指定列族和列修饰符对应的列
                }
            }
            Result result = table.get(get);
            AtomicInteger resultNum = new AtomicInteger(0);
            for (KeyValue kv : result.list()) {
//                System.out.println(Bytes.toString(kv.getValueArray()));
                resultNum.incrementAndGet();
            }
            int expectResultNum;
            //如果实际查询结果跟要查询的数量不相等,代表查询出现异常
            if(byTimeRange) {
                expectResultNum = columnNum;
            }else{
                expectResultNum = QUERY_NUM;
            }
            if (resultNum.get() != expectResultNum) {
                throw new RuntimeException("resultNum is not equal to queryNum");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        System.out.println("queryNum:" + QUERY_NUM + ",time:" + (stop - start));
    }

    public static void main(String[] args) throws IOException {
        final Testcase1 main = new Testcase1();
        main.init();
//        main.deleteTable();
//        main.createTable();
        main.batchInsert(false);
        main.batchQuery(true);
    }
}
