import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class AddressHBaseExportMapReduce extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(AddressHBaseExportMapReduce.class);
  public enum HColumnEnum {
    COL_HADDR("haddr".getBytes()), COL_WADDR("waddr".getBytes());
    private final byte[] columnName;
    HColumnEnum(byte[] column) {
      this.columnName = column;
    }
    public byte[] getColumnName() {
      return this.columnName;
    }
  }
  public static class AddressHBaseExportMapper extends
      Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
    private static final Logger LOG = LoggerFactory.getLogger(AddressHBaseExportMapper.class);
    final static byte[] COL_FAMILY = "B".getBytes();
    private String weekTime = "";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private Date weekDate;
    private long timeStamp;
    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;
    private String pathHomeInputTag = "isDefault";
    private String pathWorkInputTag = "isDefault";
    private static final int PATH_HOME_INPUT_FLAG = 1;
    private static final int PATH_WORK_INPUT_FLAG = 2;
    private static final int PATH_DEFAULT_INPUT_FLAG = 0;
    @Override
    protected void setup(
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      weekTime = conf.get("week.time");
      pathHomeInputTag = conf.get("path.home.input.tag");
      pathWorkInputTag = conf.get("path.work.input.tag");
      try {
        weekDate = sdf.parse(weekTime);
        timeStamp = weekDate.getTime();
      } catch (ParseException e) {
        LOG.error("" + e);
      }
    }
    @Override
    protected void map(LongWritable key, Text value,
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context)
        throws IOException, InterruptedException {
      int pathFlag = PATH_DEFAULT_INPUT_FLAG;
      // distinguish input path
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String inputPath = fileSplit.getPath().toString();
      if (inputPath.contains(pathHomeInputTag)) {
        pathFlag = PATH_HOME_INPUT_FLAG;
      } else if (inputPath.contains(pathWorkInputTag)) {
        pathFlag = PATH_WORK_INPUT_FLAG;
      }
      String[] fields = value.toString().split(Constant.SPLIT_SEPATATOR);
      // 数据长度key,value
      if (fields.length == 2) {
        hKey.set(fields[0].getBytes());
        if (2 == pathFlag) {
          if (!"".equals(fields[1])) {
            kv =
                new KeyValue(hKey.get(), COL_FAMILY, HColumnEnum.COL_WADDR.getColumnName(),
                    timeStamp, fields[1].getBytes());
            if (null != kv) {
              context.write(hKey, kv);
            }
          }
        }
        if (1 == pathFlag) {
          if (!"".equals(fields[1])) {
            kv =
                new KeyValue(hKey.get(), COL_FAMILY, HColumnEnum.COL_HADDR.getColumnName(),
                    timeStamp, fields[1].getBytes());
            if (null != kv) {
              context.write(hKey, kv);
            }
          }
        }
      }
    }
  }
  private static final short FULL_GRANTS = (short) 0777;
  /**
   * Sets full 777 permissions on each file.
   *
   * @param paths Array of HDFS-paths
   * @throws IOException
   */
  private static void setFullPermissions(final String... paths) throws IOException {
    LOG.info("Start change permissions");
    Configuration conf = HadoopConfiguration.getInstance();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs != null) {
      try {
        for (String path : paths) {
          Path uriPath = new Path(path + Path.SEPARATOR + "A");
          if (!hdfs.exists(uriPath)) {
            LOG.info("Path doesn't exists: " + uriPath.toString());
            continue;
          }
          LOG.info("Try to set new permissions for folder: " + uriPath.toString());
          hdfs.setPermission(uriPath, FsPermission.createImmutable(FULL_GRANTS));
          RemoteIterator<LocatedFileStatus> fileStatuses = hdfs.listLocatedStatus(uriPath);
          while (fileStatuses.hasNext()) {
            LocatedFileStatus status = fileStatuses.next();
            if (status != null) {
              LOG.info("Try to set new permissions for file: " + status.getPath());
              hdfs.setPermission(status.getPath(), FsPermission.createImmutable(FULL_GRANTS));
            }
          }
        }
      } finally {
        HdfsUtil.close(hdfs);
      }
    }
  }
  private String weekTime = "";
  private String addressHomeInput = "";
  private String addressWorkInput = "";
  private String outputPath = "";
  private String tableName = "";
  public AddressHBaseExportMapReduce() {
  }
  public AddressHBaseExportMapReduce(String weekTime, String addressHomeInput,
      String addressWorkInput, String outputPath, String tableName) {
    this.weekTime = weekTime;
    this.addressHomeInput = addressHomeInput;
    this.addressWorkInput = addressWorkInput;
    this.outputPath = outputPath;
    this.tableName = tableName;
  }
  public boolean run() throws Exception {
    String[] params = null;
    params =
        new String[] {weekTime, addressHomeInput, addressWorkInput, outputPath,
            tableName};
    int r = ToolRunner.run(new AddressHBaseExportMapReduce(), params);
    return r == 0 ? true : false;
  }
  public int run(String[] args) throws Exception {
    String weekTime = args[0];
    String addressHomeInput = args[1];
    String addressWorkInput = args[2];
    String outputPath = args[3];
    String tableName = args[4];
    Configuration conf = HadoopConfiguration.getInstance();
    conf.set("week.time", weekTime);
    conf.set("path.home.input.tag", addressHomeInput);
    conf.set("path.work.input.tag", addressWorkInput);
    conf.set("hbase.zookeeper.quorum", "192.168.254.71,192.168.254.72,192.168.254.73");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.property.maxClientCnxns", "400");
    conf.set("zookeeper.znode.parent", "/hbase");
    Configuration hbaseConf = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConf);
    Table table = connection.getTable(TableName.valueOf(tableName));
    RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
    Job job = Job.getInstance(conf, "Address Write to HBase");
    job.setJarByClass(AddressHBaseExportMapReduce.class);
    // mapper
    job.setMapperClass(AddressHBaseExportMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    // job.setOutputKeyClass(ImmutableBytesWritable.class);
    // job.setOutputValueClass(KeyValue.class);
    // job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
    // SimpleTotalOrderPartitioner.setStartKey(hbaseConf, "zu3539253256908".getBytes());
    // SimpleTotalOrderPartitioner.setEndKey(hbaseConf, "zu8699186752793".getBytes());
    // input
    job.setInputFormatClass(TextInputFormat.class);
    // job.setNumReduceTasks(50);
    // output
    // job.setOutputFormatClass(HFileOutputFormat2.class);
    // HFileOutputFormat2.configureIncrementalLoad(job, htable);
    // inputPath
    Path input = null;
    List<String> pathInputList = null;
    if (HdfsUtil.exists(addressHomeInput)) {
      pathInputList = HdfsUtil.getAllFilePaths(addressHomeInput);
      for (String path : pathInputList) {
        input = new Path(path);
        FileInputFormat.addInputPath(job, input);
      }
    }
    if (HdfsUtil.exists(addressWorkInput)) {
      pathInputList = HdfsUtil.getAllFilePaths(addressWorkInput);
      for (String path : pathInputList) {
        input = new Path(path);
        FileInputFormat.addInputPath(job, input);
      }
    }
    // outputPath
    if (HdfsUtil.exists(outputPath)) {
      HdfsUtil.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    // execute job
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    if (job.waitForCompletion(true)) {
//      ToolRunner.run(new DistCp(job.getConfiguration()), new String[] {outputPath, targetHdfsPath});
      // setFullPermissions(outputPath);
      // LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      // HTable htable = new HTable(hbaseConf, tableName);
      // loader.doBulkLoad(new Path(outputPath), htable);
      return 0;
    } else {
      return 1;
    }
  }
}
