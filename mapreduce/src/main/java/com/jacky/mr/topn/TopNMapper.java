package com.jacky.mr.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    //定义一个TreeMap作为存储数据的集合（自带按key排序功能）
    private TreeMap<FlowBean, Text> flowMap = new TreeMap<>();

    FlowBean flowBean;
    Text v;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        v = new Text();

        //1.获取一行数据
        String line = value.toString();
        //2.切分
        String[] fields = line.split("\t");
        //3.封装FlowBean
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        flowBean.set(upFlow, downFlow);

        String phoneNum = fields[1];
        v.set(phoneNum);
        //4.向TreeMap中添加数据
        flowMap.put(flowBean, v);
        //5.限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
        if (flowMap.size() > 10) {
            flowMap.remove(flowMap.lastKey());
        }
    }

    /**
     * Mapper中cleanup是MapTask程序结束时调用
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //6.遍历TreeMap集合，输出数据
        Iterator<FlowBean> beans = flowMap.keySet().iterator();
        while (beans.hasNext()) {
            FlowBean k = beans.next();
            context.write(k, flowMap.get(k));
        }
    }
}