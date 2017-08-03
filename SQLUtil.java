package tianyc.Tools;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**返回连接信息的
 * Created by adimn on 2017/6/20.
 */
public class SQLUtil {
    private static Map<String,DataSource> dataSourceMap = new HashMap<String, DataSource>();
    static{

        Properties properties = new Properties();
        //// TODO: 2017/6/16 增加分库信息
        InputStream in = null;
        try {
                in = Thread.currentThread().getContextClassLoader().getResourceAsStream("splitdb.properties");
                properties.load(in);
                Integer spnum = Integer.parseInt(properties.getProperty("splitnum"));
                for (int i=0;i<spnum;i++) {
                    String url = "url" + i;
                    String username = "username" + i;
                    String password = "password" + i;

                    ComboPooledDataSource ds = new ComboPooledDataSource();
                    ds.setDriverClass(properties.getProperty("driverClassName"));  // 参数由 Config 类根据配置文件读取
                    // 设置JDBC的URL
                    ds.setJdbcUrl(properties.getProperty(url));
                    //设置数据库的登录用户名
                    ds.setUser(properties.getProperty(username));
                    //设置数据库的登录用户密码
                    ds.setPassword(properties.getProperty(password));
                    //设置连接池的最大连接数
                    ds.setMaxPoolSize(Integer.parseInt(properties.getProperty("maxActive")));
                    ds.setCheckoutTimeout(Integer.parseInt(properties.getProperty("maxWait")));
                    ds.setAcquireRetryDelay(Integer.parseInt(properties.getProperty("removeAbandonedTimeout")));
                    ds.setTestConnectionOnCheckout(true);

                    dataSourceMap.put(i + "", ds);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally {
                if(in!=null){
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                //创建插入数据库配置信息
                in =Thread.currentThread().getContextClassLoader().getResourceAsStream("angsqldb.properties");
                properties.load(in);
                ComboPooledDataSource ds = new ComboPooledDataSource();
                ds.setDriverClass(properties.getProperty("driverClassName"));  // 参数由 Config 类根据配置文件读取
                // 设置JDBC的URL
                ds.setJdbcUrl(properties.getProperty("url"));
                //设置数据库的登录用户名
                ds.setUser(properties.getProperty("username"));
                //设置数据库的登录用户密码
                ds.setPassword(properties.getProperty("password"));
                dataSourceMap.put( "insert", ds);

                //// TODO: 2017/6/26 新增加插入数据库
                String add=properties.getProperty("insertAdd");
                if(add.equals("true")) {
                    ComboPooledDataSource ds2 = new ComboPooledDataSource();
                    // 设置JDBC的URL
                    ds2.setJdbcUrl(properties.getProperty("url2"));
                    //设置数据库的登录用户名
                    ds2.setUser(properties.getProperty("username2"));
                    //设置数据库的登录用户密码
                    ds2.setPassword(properties.getProperty("password2"));
                    dataSourceMap.put("insert2", ds2);
                }

            }catch (Exception e){
                e.printStackTrace();
            }
    }


    public static Connection getConnection() throws SQLException {
        return dataSourceMap.get("insert").getConnection();
    }

    public static Connection getConnection(String no) throws Exception {
        return dataSourceMap.get(no).getConnection();
    }

    public static DataSource getDataSource(String no) throws Exception{
        return dataSourceMap.get(no);
    }

    public static void close(Connection c , Statement s, ResultSet rs){
        if(c!=null){
            try {
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(s!=null){
            try {
                s.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
