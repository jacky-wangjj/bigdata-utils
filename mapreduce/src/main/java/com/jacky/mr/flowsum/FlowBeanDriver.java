package com.jacky.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class FlowBeanDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\ML\\Code\\bigdata-utils\\mapreduce\\src\\main\\java\\com\\jacky\\mr\\flowsum\\input\\data.txt", "D:\\data\\output"};

        Configuration conf = new Configuration();
        //1.获取job实例
        Job job = Job.getInstance(conf);
        //2.配置jar包存储位置
        job.setJarByClass(FlowBeanDriver.class);
        //3.关联mapper/reducer
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        //4.配置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.配置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //配置自定义分区
        job.setPartitionerClass(ProvincePartitioner.class);
        //配置分区数
        job.setNumReduceTasks(3);

        //6.配置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        //系统退出打印
        System.exit(result ? 0 : 1);
    }
}