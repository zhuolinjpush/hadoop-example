package cn.jpush.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import rx.Observable;
import rx.functions.Func1;
import cn.jpush.utils.SystemConfig;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class UidQueryJavaHelper {

    private static Logger logger = LogManager.getLogger(UidQueryJavaHelper.class.getName());
    private CouchbaseCluster cluster;
    private Bucket bucket;
    
    public UidQueryJavaHelper() {
        initCb();
    }
    
    public void initCb() {
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

    public void close() {
        try {
            this.bucket.close();
            this.cluster.disconnect();
        } catch (Exception e) {
            logger.error("close couchbase error", e);
        }
    }
    
    public List<JsonDocument> query(List<String> uids) {
        long start = System.currentTimeMillis();
        List<JsonDocument> data = new ArrayList<JsonDocument>();
        try {
            data = Observable
                    .from(uids)
                    .flatMap(new Func1<String, Observable<JsonDocument>>() {

                        @Override
                        public Observable<JsonDocument> call(String uid) {
                            return bucket.async().get(uid);
                        }
                        
                    })
                    .toList()
                    .toBlocking()
                    .single();
        } catch (Exception e) {
            logger.error("query error", e);
        }
        logger.info("query uid size " + uids.size() + " cost time " + (System.currentTimeMillis() - start) + " ms");
        return data;
    }
    
    public static void main(String[] args) {
       
        //test
        String line = null;
        List<String> uids = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])));
            while ((line = reader.readLine()) != null) {
                uids.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("uids size=" + uids.size());
        UidQueryJavaHelper helper = new UidQueryJavaHelper();
        List<JsonDocument> docs = helper.query(uids);
        logger.info("docs size=" + docs.size());
        for (JsonDocument doc : docs) {
            logger.info(doc.toString());
        }
        helper.close();
    }

}
