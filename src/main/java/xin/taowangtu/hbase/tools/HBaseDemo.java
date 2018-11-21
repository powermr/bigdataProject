package xin.taowangtu.hbase.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		try {
			// listTables();
			// String[] cls= {"c1","c2"};
			// createTable("hbaseJavaDemo",cls);
			// insertRow("hbaseJavaDemo","20181121152801","c1","1"," hbase java api!");
			// deleRow("hbaseJavaDemo","20181121152801","c1","1");
			// getData("hbaseJavaDemo","20181121152801","c1","1");
			// scanData("hbaseJavaDemo","20181121152801","20181121152801");
//			putImage("hbaseJavaDemo", "20181121190701", "c2", "i1", "/root/Pictures/login");
			getImage("hbaseJavaDemo", "20181121190701", "c2", "i1", "/root/Pictures/login_h");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void init() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "192.168.208.111,192.168.208.116,192.168.208.119");
		configuration.set("hbase.master", "192.168.208.210:60000");
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void close() {
		try {
			if (null != admin) {
				admin.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void listTables() throws Exception {
		// TODO Auto-generated method stub
		init();
		HTableDescriptor[] hTableDescriptor = admin.listTables();
		for (HTableDescriptor h : hTableDescriptor) {
			System.out.println(h.getNameAsString());
		}
		close();

	}

	private static void createTable(String tableName, String[] cols) throws Exception {
		init();
		TableName tablName = TableName.valueOf(tableName);
		if (admin.tableExists(tablName)) {
			System.out.println("table is exists!");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			for (String col : cols) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);

		}
		close();

	}

	private static void insertRow(String tbn, String rowkey, String cf, String c, String val) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c), Bytes.toBytes(val));

		// List<Put> putList=new ArrayList();
		// putList.add(put);
		// table.put(putList);

		table.put(put);
		table.close();
		close();
	}

	private static void deleRow(String tbn, String rowkey, String cf, String c) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		// delete colFamily
		// delete.addFamily(Bytes.toBytes(cf));
		// delete col
		// delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(c));

		table.delete(delete);

		// List<Delete> deleteList=new ArrayList<Delete>();
		// deleteList.add(delete);
		// table.delete(deleteList);

		close();

	}

	private static void getData(String tbn, String rowKey, String cf, String c) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Get get = new Get(Bytes.toBytes(rowKey));
		Result res = table.get(get);

		// get.addFamily(Bytes.toBytes(cf));
		// get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));

		showCell(res);
		table.close();
		close();

	}

	private static void showCell(Result res) {
		// TODO Auto-generated method stub
		Cell[] cells = res.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp:" + cell.getTimestamp() + " ");
			System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}

	private static void scanData(String tbn, String startRow, String stopRow) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Scan scan = new Scan();
		// scan.setStartRow(Bytes.toBytes(startRow));
		// scan.setStopRow(Bytes.toBytes(stopRow));
		ResultScanner ress = table.getScanner(scan);
		for (Result res : ress) {
			showCell(res);
		}
		table.close();

		close();
	}

	private static void putImage(String tbn, String rowKey, String cf, String c, String imgPath) throws Exception {
		init();
		FileInputStream fis = new FileInputStream(imgPath);
		byte[] bbb = new byte[fis.available()];
		fis.read(bbb);
		fis.close();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c), bbb);
		table.put(put);
		table.close();
		close();

	}

	private static void getImage(String tbn, String rowKey, String cf, String c, String outPath) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tbn));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));
		Result res=table.get(get);
		byte[] bs=res.value();
		table.close();
		File file=new File(outPath);
		FileOutputStream fos=new FileOutputStream(file);
		fos.write(bs);
		fos.close();
		close();
	}

}
