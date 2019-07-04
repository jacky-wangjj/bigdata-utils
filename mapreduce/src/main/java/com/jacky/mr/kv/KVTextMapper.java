package com.jacky.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    //hello hadoop spark    <hello, 1>
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //封装对象

        //写出
        context.write(key, v);
    }
}