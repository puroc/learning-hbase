package com.example.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by puroc on 17/5/24.
 */
public class Testcase1 {

    //    执行插入操作的线程数
    public static final int THREAD_COUNT = 50;

    //    每个插入线程插入的记录数
    public static final long ROW_COUNT_PER_THREAD = 100;

    //    一次批量插入操作的消息数
    public static final int BATCH_INSERT_NUM = 100;

    private Configuration configuration;

    //    表名
    private String tableName = "test2";

    private String columnFamily = "abc";

    //    每条记录的字段数量
    //1秒采集一次,一个月的数据量
    private static int columnNum = 2592000;
    //1秒采集一次,一年的数据量
//    private static int columnNum = 31104000;
    //半分钟采集一次,一年的数据量
//    private static int columnNum = 17280;

    //    查询操作要查询的字段数量
    public static final int QUERY_NUM = 1440;

    private static String[] columns = new String[columnNum];

    private static String[] values = new String[columnNum];

    private Admin admin;

    private Connection connection;

    public static final String COLUMN = "column-";

    public static final String VALUE = new String(new byte[32]);

    private AtomicInteger total = new AtomicInteger(0);

    private static ConcurrentHashMap<String, Work> workMap = new ConcurrentHashMap<String, Work>();

    static {
        //对插入的数据赋值
        for (int i = 0; i < columnNum; i++) {
            columns[i] = COLUMN + i;
            values[i] = VALUE;
        }
        //将不同的列分工到不同的线程
        int part = columnNum % THREAD_COUNT;
        int eachThreadCloumn = columnNum / THREAD_COUNT;
        int startIndex = 0;
        for (int i = 0; i < THREAD_COUNT; i++) {
            Work work = new Work();
            if (i == THREAD_COUNT - 1) {
                work.setStartIndex(startIndex);
                work.setEntIndex(startIndex + eachThreadCloumn + part);
            } else {
                work.setStartIndex(startIndex);
                work.setEntIndex(startIndex + eachThreadCloumn);
                startIndex += eachThreadCloumn;
            }
            workMap.putIfAbsent(i + "", work);
        }
    }

    static class Work {
        private int startIndex;

        private int entIndex;

        public int getStartIndex() {
            return startIndex;
        }

        public void setStartIndex(int startIndex) {
            this.startIndex = startIndex;
        }

        public int getEntIndex() {
            return entIndex;
        }

        public void setEntIndex(int entIndex) {
            this.entIndex = entIndex;
        }
    }

    private void init() {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "cdh1");
            configuration.set("hbase.client.pause", "200");
            configuration.set("hbase.ipc.client.tcpnodelay", "true");
            configuration.set("hbase.client.write.buffer", "100");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
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
            System.out.println("表已创建");
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
            System.out.println("表已删除");
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void insertAllColumns(HTable table, String row, List<Put> list) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        //不写wal日志,可以提高性能
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
            Counter.getInstance().add();
        }
    }

    private void insertOneColumn(String threadName, HTable table, String row, int index, List<Put> list) throws
            IOException {
        Put put = new Put(Bytes.toBytes(row));
        //不写wal日志,可以提高性能
        put.setWriteToWAL(false);
        put.add(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columns[index]),
                Bytes.toBytes(values[index]));
        list.add(put);
        Counter.getInstance().add();
        if (list.size() % BATCH_INSERT_NUM == 0) {
            table.put(list);
            table.flushCommits();
            list.clear();
//            System.out.println(threadName + ":insert " + BATCH_INSERT_NUM + " cloumns");
        }
    }


    class Task implements Runnable {

        private boolean isMultipleThread;

        private String threadName;

        private HTable table;

        public Task(String name, boolean isMultipleThread) {
            this.threadName = name;
            this.isMultipleThread = isMultipleThread;
        }

        public void run() {
            try {
                System.out.println(threadName + " start insertAllColumns.");
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlush(false);
                table.setWriteBufferSize(24 * 1024 * 1024);
                final List<Put> list = new ArrayList<Put>();
                if (isMultipleThread) {
                    insertColumnWithMultipleThread(list);
                } else {
                    insertColumnWithSingleThread(list);
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



        //每行的所有列由多个线程负责插入
        private void insertColumnWithMultipleThread(List<Put> list) {
            final Work work = workMap.get(threadName);
            for (int j = work.getStartIndex(); j < work.getEntIndex(); j++) {
                if (total.incrementAndGet() % 10000 == 0) {
                    System.out.println(total.get() + " columns has inserted.");
                }
                for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                    try {
                        String rowKey = i + "";
                        insertOneColumn(threadName, table, rowKey, j, list);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        //每个线程负责插入一行的所有列
        private void insertColumnWithSingleThread(List<Put> list) {
            for (int j = 0; j < columnNum; j++) {
                for (int i = 0; i < ROW_COUNT_PER_THREAD; i++) {
                    try {
                        String rowKey = threadName + "-" + i;
                        insertOneColumn(threadName, table, rowKey, j, list);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    private void batchInsert(boolean insertByMultiThread) throws IOException {
        Counter.getInstance().start();
        long start = System.currentTimeMillis();

        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Task(i + "", insertByMultiThread));
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

        if (insertByMultiThread) {
            System.out.println("num:" + ROW_COUNT_PER_THREAD + ",time:" + (stop - start));
        } else {
            System.out.println("num:" + ROW_COUNT_PER_THREAD * THREAD_COUNT + ",time:" + (stop - start));
        }
        Counter.getInstance().stop();
    }


    private void batchQuery(boolean byTimeRange) {
        long start = System.currentTimeMillis();
        int expectResultNum = 0;
        try {
            HTable table = new HTable(configuration, Bytes.toBytes(tableName));

            //查询第一行数据
//            Get get = new Get(Bytes.toBytes("0-9"));
            Get get = new Get(Bytes.toBytes("0"));
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

            //如果实际查询结果跟要查询的数量不相等,代表查询出现异常
            if (byTimeRange) {
                expectResultNum = columnNum;
            } else {
                expectResultNum = QUERY_NUM;
            }
            if (resultNum.get() != expectResultNum) {
                throw new RuntimeException("resultNum is not equal to queryNum");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        System.out.println("queryNum:" + expectResultNum + ",time:" + (stop - start));
    }

    public static void main(String[] args) throws IOException {
        final Testcase1 main = new Testcase1();
        main.init();
//        main.deleteTable();
//        main.createTable();
//        main.batchInsert(true);
        main.batchQuery(true);
//        for (Map.Entry<String, Work> entry : workMap.entrySet()) {
//            System.out.println(entry.getKey() + "," + entry.getValue().getStartIndex() + "," + entry.getValue()
//                    .getEntIndex());
//        }

    }
}
