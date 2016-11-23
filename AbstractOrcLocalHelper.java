package cn.msg.orc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public abstract class AbstractOrcLocalHelper {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractOrcLocalHelper.class);
    private TypeDescription schema;
    private String inputFile;
    private String hdfsFile;
    private Writer orcWriter;
    
    public AbstractOrcLocalHelper(String columns, String inputFile, String hdfsFile) {
        this.inputFile = inputFile;
        this.hdfsFile = hdfsFile;
        createStringSchema(columns);
    }
    
    protected void createStringSchema(String columns) {
        if (Strings.isNullOrEmpty(columns)) {
            logger.error("schema error");
            System.exit(0);
        }
        this.schema = TypeDescription.createStruct();
        for (String field : columns.trim().split(",")) {
            this.schema.addField(field, TypeDescription.createString());
        }
        logger.info("schema:" + this.schema.toJson() + " " + this.schema.toString());
    }

    protected void initOrcWriter() {
        try {
            Configuration conf = new Configuration();
            WriterOptions opts = OrcFile.writerOptions(conf);
            opts.setSchema(this.schema);
            opts.compress(CompressionKind.SNAPPY);
            this.orcWriter = OrcFile.createWriter(new Path(this.hdfsFile), opts);
        } catch (Exception e) {
            logger.error("init orc writer error", e);
        }
    }
    
    public abstract String[] mkline(String line);
    
    public void process() {
        File file = new File(this.inputFile);
        if (!file.exists() || !file.isFile()) {
            logger.info("file is invalid," + this.inputFile);
            System.exit(0);
        }
        initOrcWriter();
        BufferedReader reader = null;
        try {
            VectorizedRowBatch rowBatch = this.schema.createRowBatch();
            reader = new BufferedReader(new FileReader(file));
            
            String line = null;
            String[] arr = null;
            long count = 0;
            int row = 0;
            while ((line = reader.readLine()) != null) {
                arr = mkline(line);
                if (arr == null) {
                    continue;
                }
                row = rowBatch.size++;
                for (int i = 0; i < arr.length; i++) {
                    ((BytesColumnVector)rowBatch.cols[i+1]).setVal(row, arr[i].getBytes());
                }
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    orcWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
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
                this.orcWriter.close();
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
