package com.example.mr.flow;

import com.example.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean value : values) {
            sumDownFlow += value.getDownFlow();
            sumUpFlow += value.getUpFlow();
        }
        FlowBean resultBean = new FlowBean();
        resultBean.setUpFlow(sumUpFlow);
        resultBean.setDownFlow(sumDownFlow);
        resultBean.setSumFlow(sumUpFlow + sumDownFlow);
        context.write(key, resultBean);
    }
}