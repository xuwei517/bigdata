package tech.xuwei.elasticsearch;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * JDBC操作Elasticsearch SQL
 * Created by xuwei
 */
public class EsJdbcOp {
    public static void main(String[] args) throws Exception{
        //指定JDBC URL
        String jdbcUrl = "jdbc:es://http://bigdata01:9200/?timezone=UTC+8";
        Properties properties = new Properties();
        //获取JDBC连接
        Connection conn = DriverManager.getConnection(jdbcUrl, properties);
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("select name,age from user order by age desc limit 5");
        while (results.next()){
            String name = results.getString(1);
            int age = results.getInt(2);
            System.out.println(name+"--"+age);
        }

        //关闭连接
        stmt.close();
        conn.close();
    }
}
