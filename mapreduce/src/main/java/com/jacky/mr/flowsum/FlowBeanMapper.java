package com.jacky.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切分\t
        String[] fields = line.split("\t");
        int len = fields.length;
        //3.封装对象
        k.set(fields[1]);
        long upFlow = Long.parseLong(fields[len - 3]);
        long downFlow = Long.parseLong(fields[len - 2]);
        v.set(upFlow, downFlow);
        //4.写出
        context.write(k, v);
    }
}