package cn.msg.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cn.msg.helper.UserprofileThriftHelper;

import com.alibaba.fastjson.JSON;

public class ApplistOrcFixMR implements Tool {

    public static final Logger logger = Logger.getLogger(ApplistOrcFixMR.class);
    public static String TAB = "\t";
    public static String SCHEMA = "struct<imei:string,pkg:string,name:string,itime:bigint>";
    private Configuration conf;
    
    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    public static class ORCMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {
   
        private StructObjectInspector inspector;
        private Text rKey = new Text();
        private Text rVal = new Text();
         
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);
            inspector = (StructObjectInspector) OrcStruct.createObjectInspector(info);
//            inspector = (StructObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(FixApplistModel.class, 
//                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }

        @Override
        protected void map(NullWritable key, OrcStruct orc, Context context)
                throws IOException, InterruptedException {
            try {
                List<Object> list = inspector.getStructFieldsDataAsList(orc);
                String imei = ((Text) list.get(0)).toString();
                String pkg = ((Text) list.get(1)).toString();
                String name = ((Text) list.get(2)).toString();
                long itime = ((LongWritable) list.get(3)).get();
                rKey.set(imei);
                rVal.set(pkg + TAB + name + TAB + itime);
                context.write(rKey, rVal);
                
            } catch (Exception e) {
                logger.error("mapper error", e);
            }
        }

    }
    
    public static class ORCReducer extends Reducer<Text, Text, Text, Text> {

        private List<FixAppList> list = new ArrayList<FixAppList>();
        private static final String applistFQ = "C:app";
        private UserprofileThriftHelper helper = new UserprofileThriftHelper();
        private long count = 0;
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            try {
                String imei = key.toString();
                long itime = 0L;
                String[] arr = null;
                FixAppList model = null;
                for (Text tx : values) {
                    //pkg name itime
                    arr = tx.toString().split(TAB);
                    model = new FixAppList(arr[1], arr[0]);
                    list.add(model);
                    if (itime <= 0 && arr[2].length() == 10) {
                        itime = Long.parseLong(arr[2]);
                    }
                 }   
                String applist = JSON.toJSONString(list);
                if (imei.length() > 0) {
                    if (itime > 10000000) {
                        this.helper.update(imei, itime*1000, applistFQ, applist, true);
                    } else {
                        this.helper.update(imei, applistFQ, applist, true);
                    }
                }
                count++;
                if (count % 10000 == 0) {
                    logger.info(imei + " " + itime + " " + applist );
                }
                list.clear();
            } catch (Exception e) {
                logger.error("reduce error", e);
            }
            
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            logger.info("cleanup");
            helper.close();
            
        }
        
        public class FixAppList {

            private String name;
            private String pkg;
            
            public FixAppList(String name, String pkg) {
                this.name = name;
                this.pkg = pkg;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getPkg() {
                return pkg;
            }

            public void setPkg(String pkg) {
                this.pkg = pkg;
            }

        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        Job job = Job.getInstance(conf, "applist-orc-fix-" + input);
        
        job.setJarByClass(ApplistOrcFixMR.class);
        job.setMapperClass(ORCMapper.class);
        job.setReducerClass(ORCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(20);
        
        for (String s : input.split(",")) {
            FileInputFormat.addInputPath(job, new Path(s));
        }

        if (!job.waitForCompletion(true)) {
            logger.error("ailed! ");
            return -1;
        } else {
            logger.info("success! ");
            return 0;
        }
    }

    public static void main(String[] args) {
        
        if (args.length < 1 ) {
            logger.error("error args");
            return;
        } 
  
        try {
            Configuration conf = new Configuration();
            ToolRunner.run(conf, new ApplistOrcFixMR(), args);
        } catch (Exception e) {
            logger.error("mr error", e);
        }

    }
}
