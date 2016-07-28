package cn.jpush.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.jpush.common.Common;
import cn.jpush.utils.SystemConfig;

public class UidToKafkaHelper {

    private static Logger logger = LogManager.getLogger(UidToKafkaHelper.class.getName());
    private String topic = Common.TOPIC_UNKNOWNUID;
    private Properties props = new Properties();
    private Producer<String, byte[]> producer;
    
    public UidToKafkaHelper() {
        init();
    }

    private void init() {
        try {
            props.put("metadata.broker.list", SystemConfig.getProperty("kafka.metadata.broker.list"));
            props.put("request.required.acks", "-1");
            props.put("serializer.class", "kafka.serializer.DefaultEncoder");
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            props.put("producer.type", "sync");
            
            ProducerConfig producerConfig = new ProducerConfig(props);
            producer = new Producer<>(producerConfig);            
        } catch (Exception e) {
            logger.error("init kafka producer error", e);
        }
        
    }
    
    public void sendUid(String uid) {
        try {
            producer.send(new KeyedMessage<String, byte[]>(topic, uid.getBytes()));
        } catch (Exception e) {
            logger.error("produce error", e);
        }
    }

    public void sendUid(List<String> uids) {
        try {
            for (String uid : uids) {
                producer.send(new KeyedMessage<String, byte[]>(topic, uid.getBytes()));
            }
        } catch (Exception e) {
            logger.error("produce error", e);
        }
    }
    
    public void close() {
        try {
            producer.close();
        } catch (Exception e) {
            logger.error("Failed to close producer.", e);
        }
    }

    public static void main(String[] args) {
        try {
            UidToKafkaHelper helper = new UidToKafkaHelper();
            BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])));
            String line = null;
            while ((line = reader.readLine()) != null) {
                helper.sendUid(line.trim());
            }
            reader.close();
            helper.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
