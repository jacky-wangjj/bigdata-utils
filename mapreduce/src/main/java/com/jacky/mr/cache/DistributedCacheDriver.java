package com.jacky.mr.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/3
 */
public class DistributedCacheDriver {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\ML\\Code\\bigdata-utils\\mapreduce\\src\\main\\java\\com\\jacky\\mr\\cache\\input\\order.txt",
                "D:/data/output",
                "file:///D:/ML/Code/bigdata-utils/mapreduce/src/main/java/com/jacky/mr/cache/input/pd.txt"};

        Configuration conf = new Configuration();
        //1.获取job实例
        Job job = Job.getInstance(conf);
        //2.配置jar包存储位置
        job.setJarByClass(DistributedCacheDriver.class);
        //3.关联mapper/reducer
        job.setMapperClass(DistributedCacheMapper.class);
        //5.配置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.配置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //加载缓存数据
        job.addCacheFile(new URI(args[2]));
        //Map端Join，不需要Reduce阶段，reduceTask数量为0
        job.setNumReduceTasks(0);

        //7.提交job
        boolean result = job.waitForCompletion(true);
        //系统退出打印
        System.exit(result ? 0 : 1);
    }
}