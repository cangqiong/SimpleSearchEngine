package com.bigdata.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase工具类
 * 
 * @author cang
 *
 */
public class HBaseUtils {
    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    private static Configuration hbaseConfiguration = HBaseConfiguration.create();

    /**
     * 创建表
     * 
     * @param tablename 表名
     * @param columnFamily 列族
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static void CreateTable(String tablename, String[] familys) {
	try {
	    HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
	    if (admin.tableExists(tablename)) {// 如果表已经存在
		logger.info(tablename + "表已经存在!");
	    } else {
		TableName tableName = TableName.valueOf(tablename);
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for (String family : familys) {
		    tableDesc.addFamily(new HColumnDescriptor(family));
		}
		admin.createTable(tableDesc);
		logger.info(tablename + "表已经成功创建!");
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * 向表中插入一条新数据
     * 
     * @param tableName 表名
     * @param row 行键key
     * @param columnFamily 列族
     * @param column 列名
     * @param data 要插入的数据
     * @throws IOException
     */
    public static void PutData(String tableName, String row, String columnFamily, String column,
	    String data) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Put put = new Put(Bytes.toBytes(row));
	    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
	    table.put(put);
	} catch (IOException e) {
	    logger.error("Put data error: " + e.getMessage());
	}
    }

    /**
     * 向表中插入一条新数据
     * 
     * @param tableName 表名
     * @param row 行键key
     * @param columnFamily 列族
     * @param column 列名
     * @param data 要插入的数据
     */
    public static void PutData(String tableName, byte[] row, String columnFamily, String column,
	    String data) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Put put = new Put(row);
	    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
	    table.put(put);
	    table.close();
	} catch (IOException e) {
	    logger.error("Put data error: " + e.getMessage());
	}
    }

    /**
     * 判断表是否存在
     * 
     * @param tableName 表名
     */
    public static boolean tableExists(String tableName) {
	try {
	    HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
	    return admin.tableExists(tableName);
	} catch (IOException e) {
	    logger.error(e.getMessage());
	    return false;
	}
    }

    /**
     * 向表中插入一条新数据
     * 
     * @param tableName 表名
     * @param put put
     * 
     */
    public static void PutData(String tableName, Put put) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    table.put(put);
	} catch (IOException e) {
	    logger.error("Put data error: " + e.getMessage());
	}

    }

    /**
     * 判断row key是否存在
     * 
     * @param tableName 表名
     * @param row 行键key
     * @throws IOException
     */
    public static boolean checkRowExist(String tableName, String row) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Get get = new Get(Bytes.toBytes(row));
	    get.setCheckExistenceOnly(true);
	    Result result = table.get(get);
	    return result.getExists().booleanValue();
	} catch (IOException e) {
	    logger.error("Check row key exists error: " + e.getMessage());
	    return false;
	}
    }

    /**
     * 判断列数据是否存在
     * 
     * @param tableName 表名
     * @param row 行键key
     * @param column
     * @param family
     */
    public static boolean checkColumnValueExistByKey(String tableName, String row, String family,
	    String qualifier) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Get get = new Get(Bytes.toBytes(row));
	    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	    get.setCheckExistenceOnly(true);
	    Result result = table.get(get);
	    return result.getExists().booleanValue();
	} catch (IOException e) {
	    logger.error("Check row key exists error: " + e.getMessage());
	    return false;
	}
    }

    /**
     * 检查是否出现已有的数据
     * 
     * @param tableName
     * @param family
     * @param qualifier
     * @param data
     * @return
     */
    public static boolean checkColumnValueExist(String tableName, String family, String qualifier,
	    String data) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Scan scan = new Scan();
	    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
		    Bytes.toBytes(qualifier), CompareOp.EQUAL, Bytes.toBytes(data));
	    scan.setFilter(filter);
	    ResultScanner result = table.getScanner(scan);
	    Result rs = result.next();
	    if (rs != null) {
		return true;
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return false;
    }

    /**
     * 获取指定行的所有数据
     * 
     * @param tableName 表名
     * @param row 行键key
     * @param columnFamily 列族
     * @param column 列名
     * @throws IOException
     */
    public static void GetData(String tableName, String row, String columnFamily, String column)
	    throws IOException {
	HTable table = new HTable(hbaseConfiguration, tableName);
	// Scan scan = new Scan();
	// ResultScanner result = table.getScanner(scan);
	Get get = new Get(Bytes.toBytes(row));
	Result result = table.get(get);
	byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
	System.out.println(rb);
	String value = new String(rb, "UTF-8");
	logger.info("Get value =" + value);
    }

    /**
     * 获取指第一条数据
     * 
     * @param tableName 表名
     * @return
     */
    public static Result firstResult(String tableName) {
	Result result = null;
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Scan scan = new Scan();
	    ResultScanner resultScanner = table.getScanner(scan);

	    result = resultScanner.next();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return result;
    }

    /**
     * 获取指第一条指定列的数据
     * 
     * @param tableName 表名
     * @return
     */
    public static Cell firstCloumnData(String tableName, String family, String qualifier) {
	Result rs = firstResult(tableName);
	if (rs == null) {
	    return null;
	}
	Cell cell = rs.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	return cell;
    }

    /**
     * 获取指第一条指定列的数据
     * 
     * @param tableName 表名
     * @param scan 条件过滤
     * @return
     */
    public static Cell firstCloumnData(String tableName, Scan scan, String family, String qualifier) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    ResultScanner resultScanner = table.getScanner(scan);
	    Result rs = resultScanner.next();
	    if (rs != null) {
		Cell cell = rs.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return cell;
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return null;
    }

    /**
     * 获取指定表的所有数据
     * 
     * @param tableName 表名
     * @throws IOException
     */
    public static void ScanAll(String tableName) throws IOException {
	HTable table = new HTable(hbaseConfiguration, tableName);
	Scan scan = new Scan();
	ResultScanner resultScanner = table.getScanner(scan);
	for (Result result : resultScanner) {
	    List<Cell> cells = result.listCells();
	    for (Cell cell : cells) {
		byte[] rb = cell.getValueArray();
		String row = new String(result.getRow(), "UTF-8");
		String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
		String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
		String value = new String(CellUtil.cloneValue(cell), "UTF-8");
		logger.info("[row:" + row + "],[family:" + family + "],[qualifier:" + qualifier
			+ "],[value:" + value + "]");
	    }
	}
    }

    /**
     * 根据key删除一行数据
     * 
     * @param tableName
     * @param key
     * @throws IOException
     */
    public static void delete(String tableName, byte[] key) {
	try {
	    HTable table = new HTable(hbaseConfiguration, tableName);
	    Delete deleteRow = new Delete(key);
	    table.delete(deleteRow);
	    table.close();
	} catch (IOException e) {
	    logger.error("Delete data error: " + e.getMessage());
	}
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
	try {
	    // HbaseDemo.CreateTable("userinfo", "baseinfo");
	    // UrlQueueDemo.GetData("userinfo", "row2", "baseinfo", "vio3");
	    // HadoopUtils.ScanAll("url_priority_queue");
	    // HadoopUtils.delete("url_priority_queue", Bytes.toBytes("2"));
	    // HadoopUtils.ScanAll("url_priority_queue");
//	    checkColumnValueExist(urlQueue, "url", "adress", url);
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

}