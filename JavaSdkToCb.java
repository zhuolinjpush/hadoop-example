package cn.jpush.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.jpush.utils.SystemConfig;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;


public class UserInfoToCB {
    
    private static Logger logger = LogManager.getLogger(UserInfoToCB.class.getName());
    private static Pattern pattern = Pattern.compile("^[0-9]+.[0-9]+.[0-9]+$");
    private String fileName;
    private String flag;
    private CouchbaseCluster cluster;
    private Bucket bucket;
    
    public UserInfoToCB(String fileName, String flag) {
        this.fileName = fileName;
        this.flag = flag;
        initCb();
    }
    
    private void initCb() {
        int count = 0;
        while (bucket == null) {
            count += 1;
            String[] nodes = SystemConfig.getPropertyArray("sdkapp.couchbase.host");
            String bucketName = SystemConfig.getProperty("sdkapp.couchbase.bucket");
            String bucketPwd = SystemConfig.getProperty("sdkapp.couchbase.pass");
            try {
                CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                        .kvTimeout(3000)
                        .build();
                this.cluster = CouchbaseCluster.create(environment, nodes);
                this.bucket = cluster.openBucket(bucketName, bucketPwd, 5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("init couchbase bucket error", e);
            }
            
            if (count > 5) {
                logger.error("cb cluster null exit");
                break;
            }
        }
        if (null == this.cluster ) {
            logger.info("cluster is null");
        } else {
            logger.info("Init couchbase ok! count is " + count);
        }
    }

    public void process() {
        File file = new File(this.fileName);
        if (!file.exists() || !file.isFile()) {
            logger.error(String.format("%s file is invalid", this.fileName));
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            long total = 0;
            while ( (line = reader.readLine()) != null ) {
                line = line.trim();
                make(line);
                total++;
                if (total % 5000 == 0) {
                    logger.info("process count=" + total);
                }
            }
            logger.info("process end," + total);
        } catch (Exception e) {
            logger.error("process error " + fileName, e);
            initCb();
        } finally {
            close();
        }
    }

    public void make(String line) {
        try {
            if ("apkver".equals(flag)) {
                
                String[] arr = line.trim().split("\t");
                if (arr.length == 1) {
                    return;
                }
                String uid = arr[0].trim();
                String appkey = null;
                String apkver = null;
                if (arr.length == 2) {
                    if (arr[1].trim().contains(".")) {
                        apkver = arr[1].trim();
                    } else {
                        appkey = arr[1].trim();                     
                    }
                } else {
                    appkey = arr[1].trim();
                    apkver = arr[2].trim();
                }
                
                boolean exist = this.bucket.exists(uid);
                if ( exist ) {
                    JsonObject obj = this.bucket.get(uid).content();
                    obj.put("apkver", apkver).put("appkey", appkey);
                    this.bucket.upsert(JsonDocument.create(uid, obj));
                } else {
                    JsonObject obj = JsonObject.empty().put("apkver", apkver).put("appkey", appkey);
                    this.bucket.insert(JsonDocument.create(uid, obj));
                }
                
            } else if ("sdkver".equals(flag)) {
                
                String[] arr = line.trim().split("\t");
                if (arr.length == 1) {
                    return;
                }
                String channel = null;
                String sdkver = null;
                if (arr.length == 2) {
                    if (pattern.matcher(arr[1].trim()).find()) {
                        sdkver = arr[1].trim();
                    } else {
                        channel = arr[1].trim();
                    }
                } else {
                    channel = arr[1].trim();
                    sdkver = arr[2].trim();
                }
                String uid = arr[0].trim();

                boolean exist = this.bucket.exists(uid);
                if ( exist ) {
                    JsonObject obj = this.bucket.get(uid).content();
                    obj.put("sdkver", sdkver).put("channel", channel);
                    this.bucket.upsert(JsonDocument.create(uid, obj));
                } else {
                    JsonObject obj = JsonObject.empty().put("sdkver", sdkver).put("channel", channel);
                    this.bucket.insert(JsonDocument.create(uid, obj));
                }       
            } 
        } catch (Exception e) {
            logger.error("new put error," + line, e);
            initCb();
        }
    }
    
    private void close() {
        try {
            this.bucket.close();
            this.cluster.disconnect();
        } catch (Exception e) {
            logger.error("close couchbase error", e);
        }
    }
    
    public static void main(String[] args) {
        
        UserInfoToCB obj = new UserInfoToCB(args[0], args[1]);
        obj.process();        
    }

}
