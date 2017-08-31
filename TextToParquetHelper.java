package cn.jpush.hadoop.etl.util.parquet_helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.HashMap;

public abstract class AbstractParquetLocalHelper<T> {

	protected static final Logger logger = LoggerFactory.getLogger(AbstractParquetLocalHelper.class);
	private String inputFile;
	private String hdfsFile;
	private Configuration conf;

	private RecordWriter<Void, Group> writer = null;
	private SimpleGroupFactory factory = null;
	private Field[] fields = null;
	private HashMap<Field, Method> field2Getter = null;
	private CompressionCodecName codecName = CompressionCodecName.GZIP;

	public AbstractParquetLocalHelper(String inputFile, String hdfsFile) {
		this(new Configuration(), inputFile, hdfsFile);
	}

	public AbstractParquetLocalHelper(Configuration conf, String inputFile, String hdfsFile) {
		this.inputFile = inputFile;
		this.hdfsFile = hdfsFile;
		this.conf = conf;
	}

	protected String getParquetFieldType(String typeName) throws IOException {
		String pqFieldType = null;
		switch (typeName) {
			case "Byte":
			case "byte":
			case "Short":
			case "short":
			case "Integer":
			case "int": pqFieldType = "int32"; break;
			case "Long":
			case "long": pqFieldType = "int64"; break;
			case "Float":
			case "float": pqFieldType = "float"; break;
			case "Double":
			case "double": pqFieldType = "double"; break;
			case "Boolean":
			case "boolean": pqFieldType = "boolean"; break;
			case "String":  pqFieldType = "binary"; break;
			default: throw new IOException("Unsupported Type: " + typeName);
		}
		return pqFieldType;
	}

	@SuppressWarnings("unchecked")
	protected void createSchema() throws IOException {
		Type t = getClass().getGenericSuperclass();
		ParameterizedType param = (ParameterizedType) t;
		Class<T> cls = (Class<T>) param.getActualTypeArguments()[0];
		logger.info("class:" + cls);
		fields = cls.getDeclaredFields();
		Method[] methods = cls.getMethods();
		field2Getter = new HashMap<>();
		String schema = "message test{\n";
		for (Field field : fields) {
			String name = field.getName();
			String type = getParquetFieldType(field.getType().getSimpleName());
			schema += "required " + type + " " + name
					+ (type.equals("binary") ? " (UTF8);\n" : ";\n");

			// map field to getter method
			for (Method method : methods) {
				if (method.getName().equalsIgnoreCase("get" + name)) {
					field2Getter.put(field, method);
					break;
				}
			}
		}
		schema += "}";
		logger.info("schema: \n" + schema);
		conf.set("parquet.example.schema", schema);
		factory = new SimpleGroupFactory(MessageTypeParser.parseMessageType(schema));
	}

	private void initParquetWriter() {
		try {
			createSchema();
			ExampleOutputFormat outputFormat = new ExampleOutputFormat();
			this.writer = outputFormat.getRecordWriter(conf, new Path(this.hdfsFile), codecName);
		} catch (Exception e) {
			logger.error("init writer error:", e);
		}
	}

	public void setCodecName(CompressionCodecName codecName) {
		this.codecName = codecName;
	}

	public abstract T mkline(String line);

	protected Group convertFromModel2Group(T t) throws InvocationTargetException, IllegalAccessException, IOException {
		if (t == null) {
			return null;
		}
		Group group = factory.newGroup();
		for (Field field : fields) {
			Method getter = field2Getter.get(field);
			String name = field.getName();
			Object value = getter.invoke(t);
			if (value instanceof Byte) {
				group.append(name, (Byte) value);
			} else if (value instanceof Short) {
				group.append(name, (Short) value);
			} else if (value instanceof Integer) {
				group.append(name, (Integer) value);
			} else if (value instanceof Long) {
				group.append(name, (Long) value);
			} else if (value instanceof Float) {
				group.append(name, (Float) value);
			} else if (value instanceof Double) {
				group.append(name, (Double) value);
			} else if (value instanceof Boolean) {
				group.append(name, (Boolean) value);
			} else if (value instanceof String) {
				group.append(name, (String) value);
			} else {
				throw new IOException("Unsupported Type: " + field.getType());
			}
		}
		return group;
	}

	@SuppressWarnings("unchecked")
	public void process() {
		File file = new File(this.inputFile);
		if (!file.exists() || !file.isFile()) {
			logger.info("file is invalid," + this.inputFile);
			System.exit(0);
		}
		initParquetWriter();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			Group group = null;
			long count = 0;
			while ((line = reader.readLine()) != null) {
				group = convertFromModel2Group(mkline(line));
				if (group == null) {
					continue;
				}
				this.writer.write(null, group);
				count++;
				if (count % 500000 == 0) {
					logger.info("process count=" + count);
				}
			}
			logger.info("process end count=" + count);

		} catch (Exception e) {
			logger.error("process error", e);
		} finally {
			try {
				this.writer.close(null);
			} catch (IOException | InterruptedException e) {
				logger.error("close writer error", e);
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
