package com.jacky.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/3
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //id   pid amount
        //1001 01  1
        //pid   pname
        //01    xiaomi
        String line = value.toString();
        if (name.startsWith("order")) {//order表
            String[] fields = line.split(" ");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");
            k.set(fields[1]);
        } else {//产品表
            String[] fields = line.split(" ");
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");
            k.set(fields[0]);
        }
        context.write(k, tableBean);
    }
}