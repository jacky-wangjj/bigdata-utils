package com.jacky.mr.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/3
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //pid pname
            //01 xiaomi
            //切分
            String fields[] = line.split(" ");
            pdMap.put(fields[0], fields[1]);
        }
        //关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //id pid amount
        //1001 01 1
        //获取一行
        String line = value.toString();
        //切分
        String[] fields = line.split(" ");
        //获取pid
        String pid = fields[1];
        //取出pname
        String pname = pdMap.get(pid);
        //拼接
        line = line + " " + pname;
        k.set(line);
        context.write(k, NullWritable.get());
    }
}