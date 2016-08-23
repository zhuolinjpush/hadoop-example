package cn.test.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTsAndVal {
    
    private static Logger logger = LoggerFactory.getLogger(QueryTsAndVal.class);
    public static final String HTTPREPORT_TABLE = "test";
    public static final String MSGOFFLINE_TABLE = "test2";
    public static final byte[] FAMILY = Bytes.toBytes("A");
    private Connection connection;
    private Table httpReportTable;
    private Table msgOfflineTable;
    
    public QueryTsAndVal() {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            httpReportTable = connection.getTable(TableName.valueOf(HTTPREPORT_TABLE));
            msgOfflineTable = connection.getTable(TableName.valueOf(MSGOFFLINE_TABLE));
            
            
        } catch (Exception e) {
            logger.error("init error", e);
        }
    }

    public void process(String fileName, String appkey, String column) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String msgid = line.trim();
                long ts = querySendtime(msgid, appkey);
                if (ts <= 0) {
                    logger.info("error ts," + String.format("%s\t%s\t%s", msgid, appkey, ts));
                    continue;
                }
                String rowkey = msgid + appkey + ts;
                int val = queryValue(rowkey, column);
                logger.info("data:" + String.format("%s\t%s\t%s", msgid, ts, val));                
            }
            reader.close();
        } catch (Exception e) {
            logger.error("process error", e);
        }
        
    }

    public long querySendtime(String msgid, String appkey) {
        long sendtime = 0;
        try {
            Get get = new Get(Bytes.toBytes(msgid));
            get.addColumn(FAMILY, Bytes.toBytes(appkey));
            get.setMaxVersions(1);
            Result result = httpReportTable.get(get);
            if (!result.isEmpty()) {
                for (Cell cell : result.listCells()) {
                    long ts = cell.getTimestamp();
                    sendtime = ts / 1000;
                    break;
                }
            }
            logger.info(String.format("%s\t%s\t%s", msgid, appkey, sendtime));
        } catch (Exception e) {
            logger.error("query sendtime," + msgid, e);
        }
        return sendtime;
    }

    public int queryValue(String rowkey, String column) {
        int val = 0;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(FAMILY, Bytes.toBytes(column));
            get.setMaxVersions(10000);
            Result result = msgOfflineTable.get(get);
            if (!result.isEmpty()) {
                for (Cell cell : result.listCells()) {
                    val += Bytes.toInt(CellUtil.cloneValue(cell));
                }
            }
            logger.info(String.format("%s\t%s\t%s", rowkey, column, val));
        } catch (Exception e) {
            logger.error("query value error", e);
        }
        return val;
    }
    
    public void close() {
        try {
            httpReportTable.close();
            msgOfflineTable.close();
        } catch (Exception e) {
            logger.error("close error", e);
        }
    }

    public static void main(String[] args) {
        QueryTsAndVal obj = new QueryTsAndVal();
        obj.process(args[0].trim(), args[1].trim(), args[2].trim());
        obj.close();
    }

}
