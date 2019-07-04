package com.jacky.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/2
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        //获取手机号前三位
        String preHhoneNum = key.toString().substring(0, 3);
        int partition = 2;
        if ("137".equals(preHhoneNum)) {
            partition = 0;
        } else if ("182".equals(preHhoneNum)) {
            partition = 1;
        }
        return partition;
    }
}