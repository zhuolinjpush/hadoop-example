package cn.test.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class HiveJavaConn {

    public static final String S_HOUR = "hour";
    public static final String S_DAY = "day";
    public static final String S_MONTH = "month";

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHiveStats.class);

    private static final String HIVE_RUL = "jdbc:hive2:localhost:8888/app";
    private static final String HIVE_USER = "123";
    private static final String HIVE_PASS = "123";

    private static final String MYSQL_URL = "jdbc:mysql://local:3306/statsdb?rewriteBatchedStatements=true";
    private static final String MYSQL_USER = "123";
    private static final String MYSQL_PASS = "123";

    private static Map<String, String> formats = new HashMap<String, String>(3);
    static {
        formats.put(S_HOUR, "yyyyMMddHH");
        formats.put(S_DAY, "yyyyMMdd");
        formats.put(S_MONTH, "yyyyMM");
    }

    protected String statsDate;

    public AbstractHiveStats(String statsDate) {
        this.statsDate = statsDate;
    }

    public void stats() {
        prepare();
        String hql = mkHql();
        if(null == hql) {
            LOG.error("hql is null");
            throw new RuntimeException("hql is null");
        }
        LOG.info("stats hql:" + hql);
        List<BaseVO> results = queryHive(hql);
        saveResult(results);
    }

    public abstract void prepare();

    public abstract String mkHql();

    public abstract List<BaseVO> queryHive(String hql);

    public abstract void saveResult(List<BaseVO> results);

    public static String matchTimeScope(String time) {
        String scope;
        switch (time.length()) {
            case 6:
                scope = S_MONTH;
                break;
            case 8:
                scope = S_DAY;
                break;
            case 10:
                scope = S_HOUR;
                break;
            default:
                throw new RuntimeException("invalid date:" + time);
        }
        return scope;
    }

    public static boolean checkTimeOk(String scope, String time) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(formats.get(scope));
            sdf.setLenient(false);
            sdf.parse(time);
        } catch (ParseException e) {
            LOG.error("incorrect date " + time, e);
            throw new RuntimeException("incorrect date:" + time);
        }
        return true;
    }

    protected static Connection getHiveConn() {
        return getHiveConn(HIVE_RUL, HIVE_USER, HIVE_PASS);
    }

    protected static Connection getHiveConn(String url, String user, String pass) {
        return getConnection(url, user, pass, "org.apache.hive.jdbc.HiveDriver");
    }

    protected static Connection getMySqlConn() {
        return getMySqlConn(MYSQL_URL, MYSQL_USER, MYSQL_PASS);
    }

    protected static Connection getMySqlConn(String url, String user, String pass) {
        return getConnection(url, user, pass, "com.mysql.jdbc.Driver");
    }

    protected static Connection getConnection(String url, String user, String pass, String driver) {
        long st = System.currentTimeMillis();
        Connection connection = null;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            LOG.error(driver + " not found.", e);
            System.exit(-1);
        }
        int count = 0;
        int tryTimes = 5;
        while (count < tryTimes) {
            try {
                connection = DriverManager.getConnection(url, user, pass);
                break;
            } catch (SQLException e) {
                count ++;
                try {
                    Thread.sleep(5 * 60 * 1000);
                } catch (InterruptedException e1) {
                    LOG.error("sleep interrupted.", e1);
                }
                LOG.error("connect hive error. retry " + count, e);
            }
        }
        long et = System.currentTimeMillis();
        LOG.info("Create " + url + " connection cost " + (et - st));
        return connection;
    }

    protected static void closeConn(Connection conn) {
        if(null != conn) {
            try {
                LOG.info("close" + conn);
                conn.close();
            } catch (SQLException e) {
                LOG.error("close conn error", e);
            }
        }
    }

    protected static String[] splitYMDFromHour(String hour) {
        String[] result = new String[3];
        result[0] = hour.substring(0,4);
        result[1] = hour.substring(4,6);
        result[2] = hour.substring(6,8);
        return result;
    }

    protected static String[] splitYMDFromDay(String day) {
        String[] result = new String[3];
        result[0] = day.substring(0,4);
        result[1] = day.substring(4,6);
        result[2] = day.substring(6,8);
        return result;
    }

    protected static String[] splitYMFromMonth(String month) {
        String[] result = new String[3];
        result[0] = month.substring(0,4);
        result[1] = month.substring(4,6);
        return result;
    }

    protected static String getDayFromHour(String hour) {
        return hour.substring(0,8);
    }

}

