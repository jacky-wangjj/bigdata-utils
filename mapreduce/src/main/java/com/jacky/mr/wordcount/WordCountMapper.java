package com.jacky.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/1
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //hello world
        //1.获取一行
        String line = value.toString();

        //2.切分单词
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}