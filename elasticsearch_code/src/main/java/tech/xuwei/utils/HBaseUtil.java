package tech.xuwei.utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase工具类
 * Created by xuwei
 */
public class HBaseUtil {
    private HBaseUtil(){}

    private static Connection conn = getConn();

    private static Connection getConn(){
        //获取hbase链接
        Configuration conf = new Configuration();
        //指定hbase使用的zk地址
        //注意：需要在执行hbase hava代码的机器上配置zk和hbase集群的主机名和ip的映射关系
        conf.set("hbase.zookeeper.quorum","bigdata01:2181");
        //指定hbase在hdfs上的根目录
        conf.set("hbase.rootdir","hdfs://bigdata01:9000/hbase");
        //创建HBase数据库链接
        Connection co = null;
        try{
            co = ConnectionFactory.createConnection(conf);
        }catch (IOException e){
            System.out.println("获取链接失败："+e.getMessage());
        }
        return co;
    }

    /**
     * 对外提供的方法
     * @return
     */
    public static Connection getInstance(){
        return conn;
    }

    /**
     * 创建表
     * @param tableName
     * @param cfs
     */
    public static void createTable(String tableName,String... cfs) throws Exception {
        Admin admin = conn.getAdmin();
        ArrayList<ColumnFamilyDescriptor> cfArr = new ArrayList<ColumnFamilyDescriptor>();
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(cf))
                    .build();
            cfArr.add(cfDesc);
        }
        TableDescriptor tableDesc = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(tableName))
                .setColumnFamilies(cfArr)
                .build();
        admin.createTable(tableDesc);
        admin.close();
    }

    /**
     * 添加一个单元格(列)的数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws Exception
     */
    public static void put2HBaseCell(String tableName,String rowKey,String columnFamily,String column,String value)throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 向hbase中添加一批数据
     * @param tableName
     * @param list
     * @throws Exception
     */
    public static void put2HBaseList(String tableName, List<Put> list)throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(list);
        table.close();
    }

    /**
     * 根据Rowkey获取数据
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Map<String,String> getFromHBase(String tableName,String rowKey)throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        HashMap<String, String> resMap = new HashMap<String, String>();
        for (Cell cell: cells) {
            //列
            byte[] column_bytes = CellUtil.cloneQualifier(cell);
            //值
            byte[] value_bytes = CellUtil.cloneValue(cell);
            resMap.put(new String(column_bytes),new String(value_bytes));
        }
        return resMap;
    }
}
