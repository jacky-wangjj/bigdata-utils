import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.logging.Logger;

/**
 * Created by Administrator on 2018/9/21.
 */
public class HDFSUtils {
    private static final Logger logger = Logger.getLogger("HDFSUtils");

    /*
     * 从本地拷贝文件到HDFS文件系统
     */
    public void copyFromLocal(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        hdfs.copyFromLocalFile(localPath, remotePath);
        hdfs.close();
    }
    public void copyFromLocal(FileSystem hdfs, String localFilePath, String remoteFilePath) throws IOException {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        hdfs.copyFromLocalFile(localPath, remotePath);
    }
    /*
     * 重命名HDFS文件
     */
    public boolean renameFile(Configuration conf, String oldFilePath, String newFilePath) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path oldPath = new Path(oldFilePath);
        Path newPath = new Path(newFilePath);
        boolean result = hdfs.rename(oldPath, newPath);
        hdfs.close();
        return result;
    }
    public boolean renameFile(FileSystem hdfs, String oldFilePath, String newFilePath) throws IOException {
        Path oldPath = new Path(oldFilePath);
        Path newPath = new Path(newFilePath);
        boolean result = hdfs.rename(oldPath, newPath);
        return result;
    }
    /*
     * 查看文件是否存在
     */
    public boolean exits(Configuration conf, String path) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        boolean result = hdfs.exists(new Path(path));
        hdfs.close();
        return result;
    }
    public boolean exits(FileSystem hdfs, String path) throws IOException {
        boolean result = hdfs.exists(new Path(path));
        return result;
    }
    /*
     * 创建文件
     */
    public void createFile(Configuration conf, String path, byte[] contents) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream outputStream = hdfs.create(new Path(path));
        outputStream.write(contents);
        outputStream.close();
        hdfs.close();
    }
    public void createFile(Configuration conf, String path, String fileContent) throws IOException {
        createFile(conf, path, fileContent.getBytes());
    }
    public void createFile(FileSystem hdfs, String path, byte[] contents) throws IOException {
        FSDataOutputStream outputStream = hdfs.create(new Path(path));
        outputStream.write(contents);
        outputStream.close();
    }
    public void createFile(FileSystem hdfs, String path, String fileContent) throws IOException {
        createFile(hdfs, path, fileContent.getBytes());
    }
    /*
     * 删除目录或文件
     */
    public boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        boolean result = hdfs.delete(new Path(remoteFilePath), recursive);
        hdfs.close();
        return result;
    }
    public boolean deleteFile(FileSystem hdfs, String remoteFilePath, boolean recursive) throws IOException {
        boolean result = hdfs.delete(new Path(remoteFilePath), recursive);
        return result;
    }
    /*
     * 删除目录或文件（如果有子目录，则级联删除）
     */
    public boolean deleteFile(Configuration conf, String remoteFilePath) throws IOException {
        return deleteFile(conf, remoteFilePath, true);
    }
    public boolean deleteFile(FileSystem hdfs, String remoteFilePath) throws IOException {
        return deleteFile(hdfs, remoteFilePath, true);
    }
    /*
     * 创建目录
     */
    public boolean createDirectory(Configuration conf, String dirName) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path dir = new Path(dirName);
        boolean result = hdfs.mkdirs(dir);
        hdfs.close();
        return result;
    }
    public boolean createDirectory(FileSystem hdfs, String dirName) throws IOException {
        Path dir = new Path(dirName);
        boolean result = hdfs.mkdirs(dir);
        return result;
    }
    /*
     * 列出指定路径下的所有文件（不包含目录）
     */
    public RemoteIterator<LocatedFileStatus> listFiles(Configuration conf, String basePath, boolean recursive) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = hdfs.listFiles(new Path(basePath), recursive);
        //hdfs.close();
        return fileStatusRemoteIterator;
    }
    public RemoteIterator<LocatedFileStatus> listFiles(FileSystem hdfs, String basePath, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = hdfs.listFiles(new Path(basePath), recursive);
        return fileStatusRemoteIterator;
    }
    /*
     * 列出指定目录下的文件、子目录信息（非递归）
     */
    public FileStatus[] listStatus(Configuration conf, String dirPath) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] fileStatuses = hdfs.listStatus(new Path(dirPath));
        hdfs.close();
        return fileStatuses;
    }
    public FileStatus[] listStatus(FileSystem hdfs, String dirPath) throws IOException {
        FileStatus[] fileStatuses = hdfs.listStatus(new Path(dirPath));
        return fileStatuses;
    }
    /*
     * 读取文件内容
     */
    public String readFile(Configuration conf, String filePath) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(filePath);
        String fileContent = null;
        InputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        try {
            inputStream = hdfs.open(path);
            outputStream = new ByteArrayOutputStream(inputStream.available());
            IOUtils.copyBytes(inputStream, outputStream, conf);
            fileContent = outputStream.toString();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
            hdfs.close();
        }
        return fileContent;
    }
    public String readFile(FileSystem hdfs, String filePath) throws IOException {
        Path path = new Path(filePath);
        String fileContent = null;
        InputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        try {
            inputStream = hdfs.open(path);
            outputStream = new ByteArrayOutputStream(inputStream.available());
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
            fileContent = outputStream.toString();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
        return fileContent;
    }
    public Configuration getConf(String hdfsUri) {
        Configuration conf = new Configuration();
        // set FileSystem URI
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        /*conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.addResource("classpath:/core-site.xml");
        conf.addResource("classpath:/hdfs-site.xml");
        conf.addResource("classpath:/mapred-site.xml");*/
        return conf;
    }
    public FileSystem getFileSystem(URI hdfsUri, Configuration conf) throws IOException {
        FileSystem hdfs = FileSystem.get(hdfsUri, conf);
        return hdfs;
    }
    public void testEZK() throws IOException {
        String hdfsUri = "hdfs://10.110.181.6:8020";
        Configuration conf = new Configuration();
        // set FileSystem URI
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("dfs.encryption.key.provider.uri", "kms://http@node1.leap.com:16000/kms");

        URI uri = URI.create(hdfsUri);
        try {
            FileSystem hdfs1 = FileSystem.get(uri, conf, "p46_u29_ezk_1");
            hdfs1.copyFromLocalFile(new Path("D:/core-site.xml"), new Path("/user/p46_u29_ezk_1/EncryptZone"));
            hdfs1.close();
            FileSystem hdfs2 = FileSystem.get(uri, conf, "p46_u31_ezk_2");
            hdfs2.copyFromLocalFile(new Path("D:/core-site.xml"), new Path("/user/p46_u31_ezk_2/EncryptZone"));
            hdfs2.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        String hdfsUri = "hdfs://10.110.181.10:8020";
        String localFilePath = "D:/core-site.xml";
        String remoteFilePath = "hdfs://10.110.181.10:8020/my-test/";
        String newDir = "/my-test";
//        System.setProperty("HADOOP_HOME", "D:\\JavaEE\\Code\\bigdata-utils\\hadoop\\hadoop-2.6.0-cdh5.7.0");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        HDFSUtils hdfsUtils = new HDFSUtils();
        Configuration conf = hdfsUtils.getConf(hdfsUri);

        // 测试加密区内写文件
        hdfsUtils.testEZK();
        //检测路径是否存在
        if (hdfsUtils.exits(conf, newDir)) {
            System.out.println(newDir+" exits!");
        } else {
            //创建目录
            boolean result = hdfsUtils.createDirectory(conf, newDir);
            if (result) {
                logger.info(newDir+"create success!");
            } else {
                logger.info(newDir+"create fail!");
            }
        }
        String fileContent = "Hi hadoop. I miss you";
        String newFileName = newDir + "/myfile.txt";
        //创建文件
        hdfsUtils.createFile(conf, newFileName, fileContent);
        logger.info(newFileName + " create success!");
        //读取文件的内容
        String readContent = hdfsUtils.readFile(conf, newFileName);
        logger.info(newFileName + " content is:\n" + readContent);
        //获取所有目录信息
        FileStatus[] dirs = hdfsUtils.listStatus(conf, "/");
        logger.info("--根目录下的所有子目录---");
        for (FileStatus s : dirs) {
            logger.info(s.toString());
        }
        //获取所有文件
        RemoteIterator<LocatedFileStatus> files = hdfsUtils.listFiles(conf, "/", true);
        logger.info("--根目录下的所有文件---");
        while (files.hasNext()) {
            logger.info(files.next().toString());
        }
        //拷贝本地文件到hdfs文件系统
        hdfsUtils.copyFromLocal(conf, localFilePath, remoteFilePath);
        //重命名文件
        String newFilePath = newDir + "/rename-file.txt";
        hdfsUtils.renameFile(conf, newFileName, newFilePath);
        //删除文件
        boolean isDeleted = hdfsUtils.deleteFile(conf, newDir);
        if (isDeleted) {
            logger.info(newDir + "已被删除");
        } else {
            logger.info(newDir + "删除失败");
        }
        /*URI uri = URI.create(hdfsUri);
        FileSystem hdfs = hdfsUtils.getFileSystem(uri, conf);
        //检测路径是否存在
        if (hdfsUtils.exits(hdfs, newDir)) {
            System.out.println(newDir+" exits!");
        } else {
            //创建目录
            boolean result = hdfsUtils.createDirectory(hdfs, newDir);
            if (result) {
                logger.info(newDir+" create success!");
            } else {
                logger.info(newDir+"create fail!");
            }
        }
        String fileContent = "Hi hadoop. I miss you";
        String newFileName = newDir + "/myfile.txt";
        //创建文件
        hdfsUtils.createFile(hdfs, newFileName, fileContent);
        logger.info(newFileName + "create success!");
        //读取文件的内容
        String readContent = hdfsUtils.readFile(hdfs, newFileName);
        logger.info(newFileName + " content is:\n" + readContent);
        //获取所有目录信息
        FileStatus[] dirs = hdfsUtils.listStatus(hdfs, "/");
        logger.info("--根目录下的所有子目录---");
        for (FileStatus s : dirs) {
            logger.info(s.toString());
        }
        //获取所有文件
        RemoteIterator<LocatedFileStatus> files = hdfsUtils.listFiles(hdfs, "/", true);
        logger.info("--根目录下的所有文件---");
        while (files.hasNext()) {
            logger.info(files.next().toString());
        }
        //拷贝本地文件到hdfs文件系统
        hdfsUtils.copyFromLocal(hdfs, localFilePath, remoteFilePath);
        //重命名文件
        String newFilePath = newDir + "/rename-file.txt";
        hdfsUtils.renameFile(hdfs, newFileName, newFilePath);
        //删除文件
        boolean isDeleted = hdfsUtils.deleteFile(hdfs, newDir);
        if (isDeleted) {
            logger.info(newDir + "已被删除");
        } else {
            logger.info(newDir + "删除失败");
        }
        hdfs.close();*/
    }
}
