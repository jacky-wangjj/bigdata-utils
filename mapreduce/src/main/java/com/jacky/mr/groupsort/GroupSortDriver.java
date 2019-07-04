package com.jacky.mr.groupsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 题目：给你一个1G的数据文件。分别有id,name,mark,source四个字段，按照mark分组，id排序。
 * 有几个mapper？ 计算分片 1024M/128M = 8
 *
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class GroupSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\ML\\Code\\bigdata-utils\\mapreduce\\src\\main\\java\\com\\jacky\\mr\\groupsort\\input", "D:/data/output"};
        //1.获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.配置jar包路径
        job.setJarByClass(GroupSortDriver.class);
        //3.配置mapper和reducer
        job.setMapperClass(GroupSortMapper.class);
        job.setReducerClass(GroupSortReducer.class);
        //4.配置mapper输出的key和value类型
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.配置最终输出的key和value类型
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);
        //6.配置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}