package com.example.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 流量统计案例
 */
public class FlowCount {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("yarn.resoucemanager.hostname","hadoop1");
        //设置运行本地 默认就是local
//        conf.set("mapreduce.framework.name","local");
//        conf.set("fs.defaultFS","file:///");
        // 运行集群模式
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","mini1");
//        conf.set("fs.defaultFS","hdfs://hadoop1:9000");

        Job job = Job.getInstance(conf);
        //指定程序运行的入口
        job.setJarByClass(FlowCount.class);

        //指定运行是的mapper 和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //指定分区数量 reducetask
        job.setNumReduceTasks(5);

        //指定mapper输入、输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定reducer输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //指定任务文件目录/输出结果目录
        FileInputFormat.setInputPaths(job, new Path("mr/data/flowinput"));
        File file = new File("mr/data/flowinput/out");
        if (file.exists()) {
            file.delete();
        }
        FileOutputFormat.setOutputPath(job, new Path("mr/data/flowinput/out"));
        // 提交任务
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
