import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

public class HiveKerberosJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver"; 

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab("test@TEST.CN", "src/main/Resources/test.keytab");
        }catch (IOException e){
            System.out.println(e);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://127.1.1.1:10000/tmp;principal=hive/nfjd-hadoop02-node56.jpushoa.com@TEST.CN", "", "");
        Statement stmt = con.createStatement();
        String tableName = "test";

        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
        }
    }
}
