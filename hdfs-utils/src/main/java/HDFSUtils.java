import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Administrator on 2018/9/21.
 */
public class HDFSUtils {
    private static String hdfsUri = "hdfs://10.110.181.10:8020/";

    /*
     * 从本地拷贝文件到HDFS文件系统
     */
    public void copyFromLocal(FileSystem hdfs, Path src, Path dest) throws IOException {
        hdfs.copyFromLocalFile(src, dest);
    }
    /*
     * 重命名HDFS文件
     */
    public void rename(FileSystem hdfs, Path fr, Path to) throws IOException {
        hdfs.rename(fr, to);
    }
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // set FileSystem URI
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(conf);
        Path src = new Path("D:/core-site.xml");
        Path dest = new Path("hdfs://10.110.181.10:8020/user/");
        HDFSUtils hdfsUtils = new HDFSUtils();
        hdfsUtils.copyFromLocal(hdfs, src, dest);

    }
}
