package cn.msg.orc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOrcLocalHelper<T> {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractOrcLocalHelper.class);
    private String inputFile;
    private String hdfsFile;
    //hive 
    private StructObjectInspector inspector;
    private OrcSerde serde;
    private RecordWriter writer;
    private FileSystem fs;
    
    public AbstractOrcLocalHelper(String inputFile, String hdfsFile) {
        this.inputFile = inputFile;
        this.hdfsFile = hdfsFile;
    }
    
    @SuppressWarnings("unchecked")
    protected void createStringSchema() {
        Type t = getClass().getGenericSuperclass();
        ParameterizedType param = (ParameterizedType) t;
        Class<T> cls = (Class<T>) param.getActualTypeArguments()[0];
        logger.info("class:" + cls);
        this.inspector = (StructObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(cls, 
                                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        this.serde = new OrcSerde();
        logger.info(this.inspector.toString());
    }

    private void initOrcWriter() {
        try {
            createStringSchema();
            JobConf conf = new JobConf();
            fs = FileSystem.get(conf);
            OrcOutputFormat outputFormat = new OrcOutputFormat();
            this.writer = outputFormat.getRecordWriter(fs, conf, new Path(this.hdfsFile).toString(), Reporter.NULL);
        } catch (Exception e) {
            logger.error("init orc writer error", e);
        }
    }
    
    public abstract T mkline(String line);
    
    @SuppressWarnings("unchecked")
    public void process() {
        File file = new File(this.inputFile);
        if (!file.exists() || !file.isFile()) {
            logger.info("file is invalid," + this.inputFile);
            System.exit(0);
        }
        initOrcWriter();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            long count = 0;
            T t = null;
            while ((line = reader.readLine()) != null) {
                t = mkline(line);
                if (t == null) {
                    continue;
                }
                this.writer.write(NullWritable.get(), serde.serialize(t, this.inspector));
                
                count++;
                if (count % 10000 == 0) {
                    logger.info("process count=" + count);
                }
            }
            logger.info("process end count=" + count);
            
        } catch (Exception e) {
            logger.error("process error", e);
        } finally {
            try {
                this.writer.close(Reporter.NULL);
                fs.close();
            } catch (IOException e) {
                logger.error("close orc writer error", e);
            }
            try {
                if (null != reader) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.error("close reader error", e);
            }
            
        }
    }

}
