package com.jacky.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/1
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //hello,1
        //hello,1
        int sum = 0;
        //1.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);
        //2.写出hello,2
        context.write(key, v);
    }
}