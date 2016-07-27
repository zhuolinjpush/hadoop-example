package cn.jpush.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cn.jpush.common.Common;
import cn.jpush.utils.SystemConfig;

public class AppStatsThriftHelper {
    
    private static Logger logger = LogManager.getLogger(AppStatsThriftHelper.class);
    private Map<ByteBuffer, ByteBuffer> attributes;
    private static int TIMEOUT = 10000;
    private static String TABLE = Common.APP_TABLE;
    private static String FAMILY = Common.APP_FAMILY;
    private TTransport transport;
    private TProtocol protocol;
    private THBaseService.Iface client;
    private ByteBuffer table;
    private MessageDigest md;
    
    public AppStatsThriftHelper() {
        init();
    }

    public void init() {
        try {
            md = MessageDigest.getInstance("MD5");
            this.table = bytes(TABLE);
            this.attributes = new HashMap<ByteBuffer, ByteBuffer>();
            String servers = SystemConfig.getProperty("hbase.thrift.server");
            String[] host = servers.trim().split(":");
            transport = new TSocket(host[0], Integer.parseInt(host[1]), TIMEOUT);
            protocol = new TBinaryProtocol(transport);
            client= new THBaseService.Client(protocol);
            transport.open();
            logger.info("init hbase thrift client success");
        } catch (Exception e) {
            logger.error("init hbase thrift client error", e);
        }
    }
    
    public boolean save(String rowkey, long itime, double val, String family, String qualifier) {
        boolean flag = true;
        try {
            List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
            TColumnValue colVal = new TColumnValue(bytes(family), bytes(qualifier), bytes(String.valueOf(val)));
            columnValues.add(colVal);
            TPut put = new TPut(bytes(rowkey), columnValues);
            put.setTimestamp(itime);
            client.put(table, put);
        } catch (Exception e) {
            logger.error("saveMutation error", e);
            flag = false;
        }
        return flag;
    }
    
    /**
     * 
     * @param list arr: rowkey, itime, val, family, qualifier
     * @return
     */
    public boolean saveBatch(List<String[]> list) {
        boolean flag = true;
        try {
            List<TPut> puts = new ArrayList<TPut>();
            for (String[] arr : list) {
                List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
                TColumnValue colVal = new TColumnValue(bytes(arr[3]), bytes(arr[4]), bytes(arr[2]));
                columnValues.add(colVal);
                TPut put = new TPut(bytes(arr[0]), columnValues);
                put.setTimestamp(Long.parseLong(arr[1]));
                puts.add(put);
            }
            client.putMultiple(table, puts);
        } catch (Exception e) {
            logger.error("saveMutation error", e);
            flag = false;
        }
        return flag;
    }

    public Map<String, Double> query(List<String> rowkeyList, String family, String qualifier) {
        long start = System.currentTimeMillis();
        Map<String, Double> data = new HashMap<String, Double>();
        try {
            List<TGet> gets = new ArrayList<TGet>();
            List<TColumn> columns = new ArrayList<TColumn>();
            TColumn col = new TColumn(bytes(family));
            col.setQualifier(bytes(qualifier));
            columns.add(col);
            Map<String, String> mdMap = new HashMap<String, String>();
            for (String row : rowkeyList) {
                String mdRow = getMD5(row);
                mdMap.put(mdRow, row);
                TGet get = new TGet(bytes(mdRow));
                get.setColumns(columns);
                gets.add(get);
            }
            
            
            List<TResult> res = client.getMultiple(table, gets);
            for (TResult row : res) {
                double d = 0;
                String mdrow = new String(row.getRow());
                for (TColumnValue colVal : row.getColumnValues()) {
                    String qua = new String(colVal.getQualifier());
                    if (qua.equals(qualifier)) {
                        d += Double.parseDouble(new String(colVal.getValue()));
                        long ts = colVal.getTimestamp();
                        logger.info("qual:" + qua + " ts:" + ts);
                    }
                }
                data.put(mdMap.get(mdrow), d);
            }
            if (data.size() != rowkeyList.size()) {
                logger.info("query hbase data!=rowkeylist,datasize=" + data.size() + " + rowkeylist=" + rowkeyList.size());
                for (String row : rowkeyList) {
                    if (!data.containsKey(row)) {
                        data.put(row, 0.0);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("query error", e);
        }
        logger.info("query hbase size=" + rowkeyList.size()  + " " + family + ":" + qualifier + 
                ", cost " + (System.currentTimeMillis() - start));
        return data;
    }
    
    public ByteBuffer bytes(String s) {
        try {
            return ByteBuffer.wrap(s.getBytes());
        } catch (Exception e) {
            logger.error("bytes error", e);
            return null;
        }
    }
    
    public void close() {
        if (null != transport) {
            transport.close();
        }
    }
    
    public String getMD5(String key) {
        String result = null;
        try {
            if (null == md) {
                md = MessageDigest.getInstance("MD5");
            }
            md.update(key.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
        } catch (Exception e) {
            logger.error("get md5 key error", e);
        }
        return result;
    }
    
    public static void main(String[] args) {
        
        AppStatsThriftHelper obj = new AppStatsThriftHelper();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])));
            List<String> list = new ArrayList<String>();
            String line = null;
            List<String[]> batchList = new ArrayList<String[]>();
            while ((line = reader.readLine()) != null) {
                //arr: rowkey, itime, val, family, qualifier
                String[] arr = line.trim().split("\t");
                batchList.add(arr);

            }
            obj.saveBatch(batchList);
            //query
            Map<String, Double> map = obj.query(list, Common.APP_FAMILY, Common.APP_COL_OPENTIMES);
            System.out.println(map.size());
            for (String key : map.keySet()) {
                System.out.println(key + " " + map.get(key));
            }
            
            obj.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
