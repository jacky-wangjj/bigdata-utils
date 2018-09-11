import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.hadoop.hive.service.ThriftHive;

import java.util.List;

/**
 * Created by Administrator on 2018/9/11.
 */
public class ThriftCli {
    private static String host = "10.110.181.8";
    private static int port = 10000;
    public static void main(String[] args) throws Exception {
        //创建Socket连接
        final TSocket tSocket = new TSocket(host, port);
        //创建一个协议
        final TProtocol tProtocol = new TBinaryProtocol(tSocket);
        //创建Hive Client
        final ThriftHive.Client client = new ThriftHive.Client(tProtocol);
        //打开Socket
        tSocket.open();
        //执行HQL
        String sql = "show tables";
        client.execute(sql);
        //处理结果
        List<String> columns = client.fetchAll();
        for (String col : columns) {
            System.out.println(col);
        }
        //释放资源
        tSocket.close();
    }
}
