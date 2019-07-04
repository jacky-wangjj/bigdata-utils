package com.jacky.mr.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 题目：输出topN的数据
 *
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class TopNDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\ML\\Code\\bigdata-utils\\mapreduce\\src\\main\\java\\com\\jacky\\mr\\topn\\input\\data.txt", "D:\\data\\output"};

        Configuration conf = new Configuration();
        //1.获取job实例
        Job job = Job.getInstance(conf);
        //2.配置jar包存储位置
        job.setJarByClass(TopNDriver.class);
        //3.关联mapper/reducer
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        //4.配置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.配置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.配置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        //系统退出打印
        System.exit(result ? 0 : 1);
    }
}