package com.jacky.mr.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangjj17@lenovo.com
 * @date 2019/7/4
 */
public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //直接写出
        for (Text value : values) {
            context.write(value, key);
        }
    }
}