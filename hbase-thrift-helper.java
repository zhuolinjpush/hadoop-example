package cn.jpush.helper;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cn.jpush.utils.SystemConfig;

public class AppStatsThriftHelper {
    
    private static Logger logger = LogManager.getLogger(AppStatsThriftHelper.class);
    private Map<ByteBuffer, ByteBuffer> attributes;
    private static int TIMEOUT = 10000;
    private static String TABLE = "app-stats";
    private static String FAMILY = "A";
    private TTransport transport;
    private TProtocol protocol;
    private Client client;
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
            client= new Hbase.Client(protocol);
            transport.open();
            logger.info("init hbase thrift client success");
        } catch (Exception e) {
            logger.error("init hbase thrift client error", e);
        }
    }
    
    public boolean saveMutation(String rowkey, String itime, double val, String column) {
        boolean flag = true;
        try {
            List<Mutation> mutations = new ArrayList<Mutation>();
            mutations.add(new Mutation(false, bytes(FAMILY + ":" + column), bytes(String.valueOf(val)), true));
            client.mutateRow(table, bytes(getMD5(rowkey)), mutations, attributes);
        } catch (Exception e) {
            logger.error("save error", e);
            flag = false;
        }
        return flag;
    }
    
    public void updataBatch(List<String[]> list, long timestamp, boolean writeToWAL) {
        //list arr : row / family / qualifier / val
        long start = System.currentTimeMillis();
        try {
            List<BatchMutation> batchMutations = new ArrayList<BatchMutation>();
            List<Mutation> mutations = null;
            for (String[] arr : list) {
                mutations = new ArrayList<Mutation>();
                mutations.add(new Mutation(false, ByteBuffer.wrap((arr[1] + ":" + arr[2]).getBytes()), bytes(Bytes.toBytes(Integer.valueOf(arr[3]))), writeToWAL));
                batchMutations.add(new BatchMutation(bytes(Bytes.toBytes(arr[0])), mutations));
            }
            client.mutateRowsTs(LBS_TABLE, batchMutations, timestamp, attributes);  
        } catch (Exception e) {
            LOG.error("update hbase error", e);
            init();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                LOG.error("sleep error", e1);
            }//5s
        }
        LOG.info("hbase update batch size=" + list.size() + ", cost=" + (System.currentTimeMillis() - start));
    }

    public Map<String, Double> query(List<String> rowkeyList, String column) {
        long start = System.currentTimeMillis();
        Map<String, Double> data = new HashMap<String, Double>();
        try {
            List<ByteBuffer> rows = new ArrayList<ByteBuffer>();
            List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
            columns.add(bytes(column));
            Map<String, String> mdMap = new HashMap<String, String>();
            for (String row : rowkeyList) {
                String mdRow = getMD5(row);
                mdMap.put(mdRow, row);
                rows.add(bytes(mdRow));
            }
            
            List<TRowResult> res = client.getRowsWithColumns(table, rows, columns, this.attributes);
            for (TRowResult row : res) {
                String mdrow = Bytes.toString(row.getRow());
                Map<ByteBuffer, TCell> map = row.getColumns();
                double d = 0;
                for (ByteBuffer bkey : map.keySet()) {
                    TCell cell = map.get(bkey);
                    String fCol = new String(bkey.array());
                    String col = fCol.split(":")[1];
                    double val = Double.parseDouble(Bytes.toString(cell.getValue()));
                    logger.info("bkey:" + fCol + " " + cell + " " + val);
                    if (column.equals(col)) {
                        d += val;
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
        logger.info("query hbase size=" + rowkeyList.size()  + " " + column + ", cost " + (System.currentTimeMillis() - start));
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
}
