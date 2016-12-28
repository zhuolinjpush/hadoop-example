package cn.stats.userprofile;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.main.AlarmClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class UserprofileHFileMR implements Tool {

    private static Logger logger = LoggerFactory.getLogger(UserprofileHFileMR.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private Configuration conf;

    public UserprofileHFileMR() {

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
    
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            ToolRunner.run(conf, new UserprofileHFileMR(), args);
        } catch (Exception e) {
            logger.error("mr error", e);
        }

    }
    
    @Override
    public int run(String[] args) throws Exception {
        String tableName = args[0];
        String type = args[1];
        String day = args[2];
        String input = args[3];
        String output = args[4];
        logger.info(String.format("%s, %s, %s, %s, %s", tableName, type, day, input, output));
        
        conf.set("type", type);
        conf.setLong("ts", getTimestamp(day));
        conf.set("hbase.zookeeper.quorum", "192.168.254.1,192.168.254.2,192.168.254.3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.property.maxClientCnxns", "400");
        conf.set("zookeeper.znode.parent", "/hbase");
     
        Job job = Job.getInstance(conf, "userprofile-hfile-" + type + "-" + day);
        
        job.setJarByClass(UserprofileWriterMR.class);
        job.setMapperClass(HFileMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setInputFormatClass(TextInputFormat.class);

 
        for (String p : input.split(",")) {
            Path path = new Path(p);
            FileInputFormat.addInputPath(job, path);
        }
        
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        //hbase
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = hbaseConnection.getRegionLocator(TableName.valueOf(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        
        if (!job.waitForCompletion(true)) {
            logger.error("Failed, input:" + input + ", output:" + output);
            AlarmClient alarm = new AlarmClient();
            alarm.sendAlarm(48, "[userprofile-hfile-error]" + input);
            return -1;
        } else {
            logger.info("Success, input:" + input + ", output:" + output);
            return 0;
        }
    }
    
    public static long getTimestamp(String day) {
        long ts = 0;
        try {
            ts = sdf.parse(day).getTime();
        } catch (Exception e) {
            logger.error("get timestamp error", e);
        }
        return ts;
    }

    public static class HFileMapper extends 
                Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        
        private static final byte[] C_F = "C".getBytes();
        private static final byte[] deviceFQ = "devinfo".getBytes();
        private static final byte[] applistFQ = "applist".getBytes();
        private static final byte[] apptagFQ = "apptag".getBytes();
        private String type;
        private long ts;
        private long count = 0;
        private ImmutableBytesWritable rKey =  new ImmutableBytesWritable();
        private KeyValue rVal;
        
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            type = context.getConfiguration().get("type");
            ts = context.getConfiguration().getLong("ts", System.currentTimeMillis());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            count++;
            try {
                String line = value.toString();
                if ("app_tags".equals(type)) {
                    String[] arr = line.split("\t");
                    if (arr.length == 2) {
                        try {
                            String imei = arr[0].trim();
                            if (imei.length() > 0) {
                                JSONObject obj = JSON.parseObject(arr[1].trim());
                                if (obj.containsKey("app_tags")) {
                                    String val = obj.getString("app_tags").trim();
                                    rKey.set(imei.getBytes());
                                    rVal = new KeyValue(imei.getBytes(), C_F, apptagFQ,  ts, val.getBytes());
                                    if (null != rVal) {
                                        context.write(rKey, rVal);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.error("parse error", e);
                        }
                    } else {
                        logger.warn("error line:" + line);
                    }
                    
                } else if ("app_list".equals(type)) {
                    try {
                        JSONObject obj = JSON.parseObject(line);
                        if (obj.containsKey("imei") && obj.containsKey("app_list") ) {
                            String imei = obj.getString("imei").trim();
                            if (imei.length() > 0) {
                                String val = obj.getString("app_list").trim();
                                rKey.set(imei.getBytes());
                                rVal = new KeyValue(imei.getBytes(), C_F, applistFQ,  ts, val.getBytes());
                                if (null != rVal) {
                                    context.write(rKey, rVal);
                                }
                            }
                        } 
                    } catch (Exception e) {
                        logger.error("parse error", e);
                    }
                    
                } else if ("device_info".equals(type)) {
                    try {
                        JSONObject obj = JSON.parseObject(line);
                        if (obj.containsKey("imei") && obj.containsKey("device_info") ) {
                            String imei = obj.getString("imei").trim();
                            if (imei.length() > 0) {
                                String val = obj.getString("device_info").trim();
                                rKey.set(imei.getBytes());
                                rVal = new KeyValue(imei.getBytes(), C_F, deviceFQ,  ts, val.getBytes());
                                if (null != rVal) {
                                    context.write(rKey, rVal);
                                }
                            }
                        } 
                    } catch (Exception e) {
                        logger.error("parse error", e);
                    }
                } else {
                    logger.warn("unknown type: " + type);
                }
                
            } catch (Exception e) {
                logger.error("mapper error", e);
            }
            
            if (count % 5000 == 0) {
                logger.info("count=" + count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            logger.info("cleanup count=" + count);
        }

    }
}
