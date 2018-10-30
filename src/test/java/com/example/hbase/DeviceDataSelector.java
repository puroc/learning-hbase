package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.applet.Main;

import java.io.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by puroc on 2016/11/5.
 */
public class DeviceDataSelector {

    public static final String IP = "swpmcp1,swpmcp2,swpmcp4";
    public static final String PORT = "2181";
//    public static final String FROM_TIME = "1524258045087";

    public static final String FROM_TIME = "0000000000000";
    public static final String DEVICE_ID = "10042011620111073-";

    private Configuration conf;

    private String tableName = "iot_water_history";

    private String newLine = System.getProperty("line.separator");

    private Admin admin;

    private Connection connection;


    @Before
    public void setUp() throws Exception {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", PORT);
//            conf.set("hbase.zookeeper.quorum", "big-data-205");
            conf.set("hbase.zookeeper.quorum", IP);

//            conf.set("hbase.client.pause", "50");
//            conf.set("hbase.client.retries.number", "3");
//            conf.set("hbase.rpc.timeout", "2000");
//            conf.set("hbase.client.operation.timeout", "3000");
//            conf.set("hbase.client.scanner.timeout.period", "10000");

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
    public void testQueryByRowKey() throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("10042000620001001-1524258045087"));
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

    private AtomicLong total = new AtomicLong();

    @Test
    public void testScanByRangeForR() throws Exception {
        File file = new File("/Users/puroc/IdeaProjects/learning-hbase/src/test/java/com/example/hbase/data.csv");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        FileWriter fw = new FileWriter(file);
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = null;
        try {
            fw.write("time,value" + newLine);
            table = connection.getTable(TableName.valueOf(tableName));

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(DEVICE_ID + FROM_TIME)));
            Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(DEVICE_ID + System.currentTimeMillis())));

            FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            allFilters.addFilter(filter1);
            allFilters.addFilter(filter2);
            scan.setFilter(allFilters);

            rs = table.getScanner(scan);
            for (Result r : rs) {
                String value = new String(r.getValue(Bytes.toBytes("lz"), Bytes.toBytes("reading")));
                //value<0是告警
                if (Integer.parseInt(value) < 0) {
                    continue;
                }
                String time = new String(r.getValue(Bytes.toBytes("lz"), Bytes.toBytes("time")));
                fw.write(time + "," + value + newLine);
                long l = total.incrementAndGet();
            }
            System.out.println(total);
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
            if (fw != null) {
                fw.close();
            }
        }
    }




}
