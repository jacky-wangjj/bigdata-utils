package com.jacky.mr.groupsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class GroupSortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    SortBean sortBean = new SortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //id name mark source
        //获取一行
        String line = value.toString();
        //切分
        String[] fields = line.split(" ");
        //封装SortBean
        sortBean.setId(fields[0]);
        sortBean.setName(fields[1]);
        sortBean.setMark(fields[2]);
        sortBean.setSource(fields[3]);
        //写出
        context.write(sortBean, NullWritable.get());
    }
}