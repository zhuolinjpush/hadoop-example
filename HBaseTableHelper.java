package cn.msg.tool;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableHelper {

    private Connection connection;
    private Admin admin;
    
    public HBaseTableHelper() {
        init();
    }
    
    public void init() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void getTableDesc(String tableName) {
        try {
            StringBuilder builder = new StringBuilder();
            builder.append("create '").append(tableName).append("',");
                    
            //familys
            TableName tName = TableName.valueOf(tableName);
            HTableDescriptor desc = admin.getTableDescriptor(tName);
            Collection<HColumnDescriptor> families = desc.getFamilies();
            for (HColumnDescriptor colDesc : families) {
                String familyName =  colDesc.getNameAsString();
                System.out.println("Family:" + colDesc.getNameAsString());
                builder.append("{NAME => '").append(familyName).append("'},");
            }
            //splits
            builder.append("{SPLITS => [");
            List<HRegionInfo> regions = admin.getTableRegions(tName);
            int len = regions.size();
            int i = 1;
            for (HRegionInfo region : regions) {
                String regionName = region.getRegionNameAsString();
                String endKey = Bytes.toString(region.getEndKey());
                System.out.println("Region:" + regionName + ", Endkey:" + endKey);
                if (!endKey.trim().equals("")) {
                    builder.append("'").append(endKey).append("'");
                    if (i < len - 1) {
                        builder.append(",");
                    } 
                    i++;
                }  
            }
            builder.append("]}");
            System.out.println(builder.toString());
            
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    public static void main(String[] args) {
        HBaseTableHelper helper = new HBaseTableHelper();
        helper.getTableDesc(args[0]);
    }

}
