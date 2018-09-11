import java.sql.*;

/**
 * Created by Administrator on 2018/9/11.
 */
public class JDBCUtils {
    //hiveServer2
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://10.110.181.8:10000/default";
    //hiveServer
    private static String driver0 = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url0 = "jdbc:hive://10.110.181.8:10000/default";
    private static String user = "hive";
    private static String password = "";
    //注册驱动
    static {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    //获取连接
    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    //释放资源
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                rs = null;
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                st = null;
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn = null;
            }
        }
    }
    public static void main(String[] args) {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        String sql = "show tables";

        try {
            //获取连接
            conn = JDBCUtils.getConnection();
            //创建运行环境
            st = conn.createStatement();
            //运行HQL
            rs = st.executeQuery(sql);
            //处理数据
            while (rs.next()) {
                String tableName = rs.getString(1);
                System.out.println(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //释放资源
            JDBCUtils.release(conn, st, rs);
        }
    }
}
