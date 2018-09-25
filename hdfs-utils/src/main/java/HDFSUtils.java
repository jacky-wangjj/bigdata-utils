import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Administrator on 2018/9/21.
 */
public class HDFSUtils {
    //private static String hdfsUri = "hdfs://10.110.181.10:8020";
    private static String hdfsUri = "hdfs://10.110.181.10:9000";

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
    /*
     * 查看文件是否存在
     */
    public boolean exits(Configuration conf, String path) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        boolean result = hdfs.exists(new Path(path));
        hdfs.close();
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
    /*
     * 删除目录或文件
     */
    public boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        boolean result = hdfs.delete(new Path(remoteFilePath), recursive);
        hdfs.close();
        return result;
    }
    /*
     * 删除目录或文件（如果有子目录，则级联删除）
     */
    public boolean deleteFile(Configuration conf, String remoteFilePath) throws IOException {
        return deleteFile(conf, remoteFilePath, true);
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
    /*
     * 列出指定路径下的所有文件（不包含目录）
     */
    public RemoteIterator<LocatedFileStatus> listFiles(Configuration conf, String basePath, boolean recursive) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = hdfs.listFiles(new Path(basePath), recursive);
        hdfs.close();
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
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // set FileSystem URI
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String localFilePath = "D:/core-site.xml";
        //String remoteFilePath = "hdfs://10.110.181.10:8020/my-test/";
        String remoteFilePath = "hdfs://10.110.181.10:9000/my-test/";
        String newDir = "/my-test";
        HDFSUtils hdfsUtils = new HDFSUtils();
        //检测路径是否存在
        if (hdfsUtils.exits(conf, newDir)) {
            System.out.println(newDir+" exits!");
        } else {
            //创建目录
            boolean result = hdfsUtils.createDirectory(conf, newDir);
            if (result) {
                System.out.println(newDir+"create success!");
            } else {
                System.out.println(newDir+"create fail!");
            }
        }
        String fileContent = "Hi hadoop. I miss you";
        String newFileName = newDir + "/myfile.txt";
        //创建文件
        hdfsUtils.createFile(conf, newFileName, fileContent);
        System.out.println(newFileName + "create success!");
        //读取文件的内容
        String readContent = hdfsUtils.readFile(conf, newFileName);
        System.out.println(newFileName + " content is:\n" + readContent);
        //获取所有目录信息
        FileStatus[] dirs = hdfsUtils.listStatus(conf, "/");
        System.out.println("--根目录下的所有子目录---");
        for (FileStatus s : dirs) {
            System.out.println(s);
        }
        //获取所有文件
        RemoteIterator<LocatedFileStatus> files = hdfsUtils.listFiles(conf, "/", true);
        System.out.println("--根目录下的所有文件---");
        while (files.hasNext()) {
            System.out.println(files.next());
        }
        //拷贝本地文件到hdfs文件系统
        hdfsUtils.copyFromLocal(conf, localFilePath, remoteFilePath);
        //重命名文件
        String newFilePath = newDir + "/rename-file.txt";
        hdfsUtils.renameFile(conf, newFileName, newFilePath);
        //删除文件
        boolean isDeleted = hdfsUtils.deleteFile(conf, newDir);
        if (isDeleted) {
            System.out.println(newDir + "已被删除");
        } else {
            System.out.println(newDir + "删除失败");
        }
    }
}
