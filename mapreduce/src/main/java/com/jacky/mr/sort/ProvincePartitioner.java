package com.jacky.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text value, int numPartitions) {
        //获取手机号前三位
        String preHhoneNum = value.toString().substring(0, 3);
        int partition = 2;
        if ("137".equals(preHhoneNum)) {
            partition = 0;
        } else if ("182".equals(preHhoneNum)) {
            partition = 1;
        }
        return partition;
    }
}