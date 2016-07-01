package cn.jpush.helper;

import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import cn.jpush.common.Common;
import cn.jpush.utils.SystemConfig;
import cn.jpush.vo.UidInfo;

public class QueryUidHelper {

    private static Logger logger = LogManager.getLogger(QueryUidHelper.class); 
    private Map<ByteBuffer, ByteBuffer> attributes = new HashMap<ByteBuffer, ByteBuffer>();
    private static int TIMEOUT = 10000;
    private TTransport transport;
    private TProtocol protocol;
    private Client client;
    private ByteBuffer table;
    
    public QueryUidHelper() {
        init();
        this.table = bytes(Common.UID_INFO_TABLE);
    }

    public void init() {
        try {
            String servers = SystemConfig.getProperty("hbase.thrift.server");
            String[] server = servers.split(",");
            String[] host = server[(int)(Math.random() * server.length)].split(":");
            transport = new TSocket(host[0], Integer.parseInt(host[1]), TIMEOUT);
            protocol = new TBinaryProtocol(transport);
            client= new Hbase.Client(protocol);
            transport.open();
            logger.info("init hbase thrift client success");
        } catch (Exception e) {
            logger.error("init hbase thrift client error", e);
        }
    }
    
    public List<UidInfo> query(Set<String> uidList) {
        long start = System.currentTimeMillis();
        List<UidInfo> data = new ArrayList<UidInfo>();
        try {
            Map<String, String> uidMap = new HashMap<String, String>();
            List<ByteBuffer> rows = new ArrayList<ByteBuffer>();
            for (String uid : uidList) {
                String rUid = new StringBuilder(uid).reverse().toString();
                uidMap.put(rUid, uid);
                rows.add(bytes(rUid));
            }
            
            List<TRowResult> res = client.getRows(table, rows, this.attributes);
            for (TRowResult row : res) {
                String ruid = Bytes.toString(row.getRow());
                UidInfo vo = new UidInfo(uidMap.get(ruid));
                Map<ByteBuffer, TCell> map = row.getColumns();
                for (ByteBuffer bkey : map.keySet()) {
                    TCell cell = map.get(bkey);
                    String fCol = new String(bkey.array());
                    String col = fCol.split(":")[1];
                    String val = Bytes.toString(cell.getValue());
                    logger.info("bkey:" + fCol + " " + cell + " " + val);
                    if (col.equals(Common.APPKEY)) {
                        vo.setAppkey(val);
                    } else if (col.equals(Common.APKVER)) {
                        vo.setApkver(val);
                    } else if (col.equals(Common.SDKVER)) {
                        vo.setSdkver(val);
                    } else if (col.equals(Common.CHANNEL)) {
                        vo.setChannel(val);
                    }       
                }
                data.add(vo);
            }
        } catch (Exception e) {
            logger.error("query error", e);
        }
        logger.info("query hbase size=" + uidList.size() + " cost " + (System.currentTimeMillis() - start));
        return data;
    }
    
    public ByteBuffer bytes(String s) {
        try {
            return ByteBuffer.wrap(s.getBytes(Common.CHARSET));
        } catch (UnsupportedEncodingException e) {
            logger.error("bytes error", e);
            return null;
        }
    }
    
    public void close() {
        if (null != transport) {
            transport.close();
        }
    }

    public static void main(String[] args) {
        Set<String> uids = new HashSet<String>();
        uids.add("100000736");
        uids.add("900006528");
        uids.add("900006560");
        uids.add("900006592");
        QueryUidHelper helper = new QueryUidHelper();
        List<UidInfo> data = helper.query(uids);
        for (UidInfo v : data) {
            v.printInfo();
        }
    }

}
