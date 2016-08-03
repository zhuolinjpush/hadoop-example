package cn.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public abstract class AbstractKafkaExecutorMsgBatch {
    
    protected static Logger LOG = LoggerFactory.getLogger(AbstractKafkaExecutorMsgBatch.class);
    private static boolean SHUTDOWN_BY_SIGNAL = false;
    private final String m_zookeeper;
    private final String m_topic;
    private final String m_groupId;
    private KafkaStream<byte[], byte[]> m_stream;
    private ConsumerConnector consumer;
    protected List<Object> msgBatch = new ArrayList<Object>();

    private int batchMill = 2 * 1000;
    private long preMill = 0;
    
    public AbstractKafkaExecutorMsgBatch(String m_zookeeper, String m_topic, String m_groupId, int second) {
        this.m_zookeeper = m_zookeeper;
        this.m_topic = m_topic;
        this.m_groupId = m_groupId;
        this.batchMill = 1000 * second;
        openStream();
    }
    
    public abstract List<Object> handle(String body);
    public abstract void batchCommit();
    public abstract void cleanup();
    
    public void process() {
        registerShutdown();
        ConsumerIterator<byte[], byte[]> it = null;
        try {
            it = m_stream.iterator();
            while (it.hasNext() && !SHUTDOWN_BY_SIGNAL) {
                try {
                    byte[] ser = it.next().message();
                    if(null != ser) {
                        String body = new String(ser);
                        LOG.info("data:" + body);
                        List<Object> obj = handle(body);
                        if (obj != null && obj.size() > 0) {
                            msgBatch.addAll(obj);
                        }
                        long currMill = System.currentTimeMillis();
                        if ((currMill - preMill) > batchMill) {
                            if(msgBatch.isEmpty()) {
                                LOG.info("execute queue is empty.");
                            } else {
                                batchCommit();
                            }
                            preMill = currMill;
                        }
                    }
                } catch (Exception e) {
                    LOG.error("while m_stream error", e);
                }
                
                if ( SHUTDOWN_BY_SIGNAL ) {
                    LOG.info("kafka shutdown");
                    shutdownConsumer();
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error("process error, reconnect kafka", e);
            openStream();
        }
        
        LOG.info("shutdown next stream");
        while (it != null && it.hasNext()) {
            byte[] ser = it.next().message();
            if(null != ser) {
                String body = new String(ser);
                LOG.info("api-data:" + body);
                Object obj = handle(body);
                if (obj != null) {
                    msgBatch.add(obj);
                } 
                if (msgBatch.size() >= 500) {
                    batchCommit();
                }
            }
        }
        
        if (!msgBatch.isEmpty()) {
            LOG.info("msg batch end");
            batchCommit();
        }
        
        cleanup();
        LOG.info("process finish");
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("offsets.storage", "kafka");
        props.put("dual.commit.enabled", "false");
        props.put("auto.commit.interval.ms", "2000");
        props.put("zookeeper.session.timeout.ms", "12000");
        props.put("zookeeper.connection.timeout.ms", "10000");
        props.put("zookeeper.connect", m_zookeeper);
        props.put("group.id", m_groupId);
        return new ConsumerConfig(props);
    }

    private void openStream() {
        try{
            shutdownConsumer();
            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(m_topic, new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_topic);
            LOG.info("stream size is " + streams.size());
            m_stream = streams.get(0);
        } catch (Exception e) {
            LOG.error("Failed to open stream", e);
        }
    }
    
    private void shutdownConsumer() {
        if(null != consumer) {
            try {
                consumer.shutdown();
                LOG.info("consumer close");
            } catch (Exception e) {
                LOG.error("Failed to shutdown consumer", e);
            }
        }
    }
    
    @SuppressWarnings("restriction")
    private static void registerShutdown() {
        SignalHandler handler = new SignalHandler() {
            public void handle(Signal signal) {
                if ( SHUTDOWN_BY_SIGNAL ){
                    //System.out.println("Wait for exit...");
                } else {
                    LOG.info("Shutdown handler - " + signal.getName() );
                    //System.out.println("Shutdown handler - " + signal.getName());
                }
                SHUTDOWN_BY_SIGNAL = true;
            }
        };
        Signal.handle(new Signal("INT"), handler);
        Signal.handle(new Signal("TERM"), handler);
        LOG.info("registerShutdown OK!");
    }

}
