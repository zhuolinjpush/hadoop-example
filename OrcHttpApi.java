package cn.msg.orc;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.jpush.msg.orc.model.HttpApiModel;

public class OrcHttpApi extends AbstractOrcLocalHelper<HttpApiModel> {
    
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

    public OrcHttpApi(String inputFile, String hdfsFile) {
        super(inputFile, hdfsFile); 
    }

    @Override
    public HttpApiModel mkline(String line) {
        //appkey,msgid,itime,msg_type,receiver_type,errno,platform,imon,iday,ihour
        HttpApiModel data = null;
        try {
            data = new HttpApiModel();
            String[] arr = line.trim().split("\t");
            data.setAppkey(arr[0].trim());
            data.setMsgid(Long.parseLong(arr[1]));
            data.setItime(Long.parseLong(arr[2]));
            data.setMsg_type(arr[3]);
            data.setReceiver_type(arr[4]);
            data.setErrno(arr[5]);
            data.setPlatform(arr[6]);
            String ihour = null;
            if (arr[2].length() == 10) {
                ihour = sdf.format(new Date(Long.parseLong(arr[2]) * 1000));
            } else {
                ihour = sdf.format(new Date(Long.parseLong(arr[2])));
            }
            //imon iday ihour
            data.setImon(Integer.parseInt(ihour.substring(0, 6)));
            data.setIday(Integer.parseInt(ihour.substring(0, 8)));
            data.setIhour(Integer.parseInt(ihour));
        } catch (Exception e) {
            logger.error("mkline error," + line, e);
            data = null;
        }
        return data;
    }
    
    public static void main(String[] args) {
        OrcHttpApi obj = new OrcHttpApi(args[0], args[1]);
        obj.process();      
    }

}
