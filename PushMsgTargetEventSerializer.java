package cn.flume;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class PushMsgTargetEventSerializer implements EventSerializer {

    private static Logger logger = LoggerFactory.getLogger(PushMsgTargetEventSerializer.class);
    private static final String HIVE_SEP = "\u0001";
    private static Set<String> target_Steps = new HashSet<String>();
    private StringBuilder builder = new StringBuilder();
    
    private final OutputStream out;

    public PushMsgTargetEventSerializer(OutputStream out, Context ctx) {
        this.out = out;

        target_Steps.add("seg-to-pushtask");
        target_Steps.add("seg-to-apns");
        target_Steps.add("seg-to-mpns");
        target_Steps.add("seg-to-mipns");
        target_Steps.add("seg-to-hpns");
        target_Steps.add("seg-to-mzpns");
        
    }

    @Override
    public void write(Event event) throws IOException {
        try {
            String body = new String(event.getBody());
            JSONObject obj = JSON.parseObject(body);
            if (obj.get("rows") == null) {  
                process(obj); 
            } else {
                JSONArray arr = obj.getJSONArray("rows");
                for (int index = 0; index < arr.size(); index ++) {
                    process( arr.getJSONObject(index) );
                }
            }
            
        } catch (Exception e) {
            logger.error("process error", e);
        }
        
    }
    
    /**
     * process JsonObject
     * @param obj
     */
    public void process(JSONObject obj) {
        try {
            builder.delete(0, builder.length());
            //{"appkey":"430b0953ad00145b6e996eb7","extra":{"my_ip":"192.168.248.108"},"itime":1494496411800,"msg_id":7274119505,
            //"platform":"a","step":"seg-to-mipns","uid":9144299385}
            long msgid = obj.getLong("msg_id");
            if(msgid == 1) {
                return ;
            } 
            String step = obj.getString("step").trim();
            if (!target_Steps.contains(step)) {
                return;
            }
            String appkey = "null";
            if (obj.containsKey("appkey")) {
                appkey = obj.getString("appkey").trim();
            }
            String platform = "null";
            if (obj.containsKey("platform")) {
                platform = obj.getString("platform");
            }
            long itime = obj.getLong("itime");
            long uid = obj.getLong("uid");
            
            builder.append(appkey).append(HIVE_SEP)
                .append(platform).append(HIVE_SEP)
                .append(msgid).append(HIVE_SEP)
                .append(uid).append(HIVE_SEP)
                .append(itime);
            
            this.out.write(builder.toString().getBytes());     
            this.out.write('\n');

        } catch (Exception e) {
            logger.error("process error", e);
        }  
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            PushMsgTargetEventSerializer s = new PushMsgTargetEventSerializer(out, context);
          return s;
        }

    }
  

    @Override
    public void afterCreate() throws IOException {
        //noop
    }


    @Override
    public void afterReopen() throws IOException {
        //noop
    }


    @Override
    public void beforeClose() throws IOException {
        //noop
    }


    @Override
    public void flush() throws IOException {
        //noop
    }


    @Override
    public boolean supportsReopen() {
        return true;
    }

}
