package com.jacky.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/3
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    protected FSDataOutputStream fosbaidu;
    protected FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {
        try {
            //1.获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //2.创建输出到baidu.log
            fosbaidu = fs.create(new Path("D:\\data\\baidu.log"));
            //3.创建输出到other.log
            fosother = fs.create(new Path("D:\\data\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        //判读key中是否有baidu，如果有则写出到baidu.log，否则写入other.log
        if (key.toString().contains("baidu")) {
            fosbaidu.write(key.toString().getBytes());
        } else {
            fosother.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭资源
        IOUtils.closeStream(fosbaidu);
        IOUtils.closeStream(fosother);
    }
}