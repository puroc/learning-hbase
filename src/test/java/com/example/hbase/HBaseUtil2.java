package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Administrator
 *
 */
public class HBaseUtil2 {

	public static Configuration configuration;
	public static Connection con;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "master,slave1");
//		configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
//		configuration.set("hbase.zookeeper.quorum", "10.10.20.209,10.10.20.207,10.10.20.205");
		
		try {
			con = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			QueryAll("test");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 查询�?��数据
	 * 
	 * @param tableName
	 *            aa
	 * @throws IOException
	 */
	public static void QueryAll(String tableName) throws IOException {
		Connection connection = ConnectionFactory
				.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(tableName));
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

	/**
	 * 单条件按查询，查询多条记�?
	 * 
	 * @param tableName
	 */
	public static void QueryByCondition2(String tableName) {

		try {
			HTable table = (HTable) con.getTable(TableName.valueOf(tableName));
			Filter filter = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // 当列column1的�?为aaa时进行查�?
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
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
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 组合条件查询
	 * 
	 * @param tableName
	 */
	public static void QueryByCondition3(String tableName) {

		try {
			HTable table = (HTable) con.getTable(TableName.valueOf(tableName));

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(
					Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(
					Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
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
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}