package com.jacky.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class SequenceFileDriver {

    //将多个小文件合并成sequenceFile文件 part-r-00000
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //1.获取job实例
        Job job = Job.getInstance(conf);
        //2.配置jar包存储位置
        job.setJarByClass(SequenceFileDriver.class);
        //3.关联mapper/reducer
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //配置输入inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //配置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //4.配置mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.配置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.配置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        //系统退出打印
        System.exit(result ? 0 : 1);
    }
}