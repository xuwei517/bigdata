package tech.xuwei.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作HBase
 * 表：创建、删除
 * 数据：增、删、改、查
 * Created by xuwei
 */
public class HBaseOp {
    public static void main(String[] args) throws Exception {
        //获取HBase数据库连接
        Connection conn = getConn();
        //添加数据
        //put(conn);

        //查询数据
        //get(conn);

        /**
         * 查询多版本的数据
         * 当列的值有多个历史版本的时候
         *
         * 修改列族info的最大历史版本存储数量
         * alter 'student',{NAME=>'info',VERSIONS=>3}
         *
         * 然后再执行下面命令，向列族info中的age列中添加几次数据，实现多历史版本数据存储
         * put 'student','laowang','info:age','19'
         * put 'student','laowang','info:age','20'
         *
         */
        //getMoreVersion(conn);

        //修改数据--同添加数据

        //删除数据
        //delete(conn);

        //==============================分割线======================

        //获取管理权限，负责对HBase中的表进行操作（DDL操作）
        Admin admin = conn.getAdmin();
        //创建表
        //createTable(admin);

        //删除表
        //deleteTable(admin);

        //关闭admin连接
        admin.close();
        //关闭连接
        conn.close();

    }

    /**
     * 删除表
     * @param admin
     * @throws IOException
     */
    private static void deleteTable(Admin admin) throws IOException {
        //删除表，先禁用表
        admin.disableTable(TableName.valueOf("test"));
        admin.deleteTable(TableName.valueOf("test"));
    }

    /**
     * 创建表
     * @param admin
     * @throws IOException
     */
    private static void createTable(Admin admin) throws IOException {
        //指定列族信息
        ColumnFamilyDescriptor familyDesc1 = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("info"))
                //在这里可以给列族设置一些属性
                .setMaxVersions(3)	//指定最多存储多少个历史版本数据
                .build();
        ColumnFamilyDescriptor familyDesc2 = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("level"))
                //在这里可以给列族设置一些属性
                .setMaxVersions(2)	//指定最多存储多少个历史版本数据
                .build();
        ArrayList<ColumnFamilyDescriptor> f = new ArrayList<ColumnFamilyDescriptor>();
        f.add(familyDesc1);
        f.add(familyDesc2);

        //获取TableDescriptor对象
        TableDescriptor desc = TableDescriptorBuilder
                .newBuilder(TableName.valueOf("test"))	//指定表名
                .setColumnFamilies(f)	//指定列族
                .build();
        //创建表
        admin.createTable(desc);
    }

    /**
     * 删除数据
     * @param conn
     * @throws IOException
     */
    private static void delete(Connection conn) throws IOException {
        //获取Table对象，指定要操作的表名，表需要提前创建好
        Table table = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，返回Delete对象
        Delete delete = new Delete(Bytes.toBytes("laowang"));
        //【可选】可以在这里指定要删除指定Rowkey数据哪些列族中的列
        //delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));

        table.delete(delete);
        //关闭table连接
        table.close();
    }

    /**
     * 查询多版本的数据
     * @param conn
     * @throws IOException
     */
    private static void getMoreVersion(Connection conn) throws IOException {
        //获取Table对象，指定要操作的表名，表需要提前创建好
        Table table = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，返回Get对象
        Get get = new Get(Bytes.toBytes("laowang"));
        //读取cell中的所有历史版本数据，不设置此配置时默认读取最新版本的数据
        //可以通过get.readVersions(2)来指定获取多少个历史版本的数据
        get.readAllVersions();

        Result result = table.get(get);

        //获取指定列族中指定列的所有历史版本数据，前提是要设置get.readAllVersions()或者get.readVersions(2)，否则只会获取最新数据
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age"));
        for (Cell cell :columnCells) {
            //其实获取Cell中的value也可以使用
            byte[] value_bytes = CellUtil.cloneValue(cell);
            long timestamp = cell.getTimestamp();
            System.out.println("值为："+new String(value_bytes)+",时间戳："+timestamp);
        }
        //关闭table连接
        table.close();
    }

    /**
     * 查询数据
     * @param conn
     * @throws IOException
     */
    private static void get(Connection conn) throws IOException {
        //获取Table对象，指定要操作的表名，表需要提前创建好
        Table table = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，返回Get对象
        Get get = new Get(Bytes.toBytes("laowang"));
        //【可选】可以在这里指定要查询指定Rowkey数据哪些列族中的列
        // 如果不指定，则默认查询指定Rowkey所有列的内容
        //get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));
        //get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"));

        Result result = table.get(get);
        //如果不清楚HBase中到底有哪些列族和列，则可以使用listCells()方法获取所有cell（单元格），cell对应的是某一个列的数据
        List<Cell> cells = result.listCells();
        for (Cell cell: cells) {
            //提示：下面获取的信息都是字节类型的，可以通过new String(bytes)方法将其转为字符串
            //列族
            byte[] famaily_bytes = CellUtil.cloneFamily(cell);
            //列
            byte[] column_bytes = CellUtil.cloneQualifier(cell);
            //值
            byte[] value_bytes = CellUtil.cloneValue(cell);
            System.out.println("列族："+new String(famaily_bytes)+",列："+new String(column_bytes)+",值："+new String(value_bytes));
        }
        System.out.println("===================================================");
        //如果明确知道HBase中有哪些列族和列，则可以使用getValue(family, qualifier)方法直接获取指定列族中指定列的数据
        byte[] age_bytes = result.getValue(Bytes.toBytes("info"),Bytes.toBytes("age"));
        System.out.println("age列的值："+new String(age_bytes));
        //关闭table连接
        table.close();
    }

    /**
     * 添加数据
     *
     * @param conn
     * @throws IOException
     */
    private static void put(Connection conn) throws IOException {
        //获取Table对象，指定要操作的表名，表需要提前创建好
        Table table = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，返回put对象
        Put put = new Put(Bytes.toBytes("laowang"));
        //向put对象中指定列族、列、值
        //put 'student','laowang','info:age','18'
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        //put 'student','laowang','info:sex','man'
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes("man"));
        //put 'student','laowang','level:class','A'
        put.addColumn(Bytes.toBytes("level"),Bytes.toBytes("class"),Bytes.toBytes("A"));
        //向表中添加数据
        table.put(put);
        //关闭table连接
        table.close();
    }

    /**
     * 获取连接
     *
     * @return
     * @throws IOException
     */
    private static Connection getConn() throws IOException {
        //获取配置
        Configuration conf = HBaseConfiguration.create();
        //指定HBase使用的Zookeeper的地址，多个地址都逗号隔开
        conf.set("hbase.zookeeper.quorum", "bigdata01:2181,bigdata02:2181,bigdata03:2181");
        //指定HBase在HDFS上的根目录
        conf.set("hbase.rootdir","hdfs://bigdata01:9000/hbase");
        //创建HBase连接，负责对HBase中数据的增删改查（DML操作）
        return ConnectionFactory.createConnection(conf);
    }
}
