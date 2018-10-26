package com.example.mr.table_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 表合并例子
 * <p>
 * mapper reducer方式
 * 整合数据中pid相同的数据
 */
public class TableJoin {

    static class TableJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {

        InfoBean bean = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //通过文件名来判断是那张表的数据
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] fields = line.split(",");
            String pid;
            if ("order".startsWith(fileName)) {
                //订单表
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");

            } else {
                //商品表
                pid = fields[0];
                bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
            }

            k.set(pid);

            context.write(k, bean);
        }
    }

    static class TableJoinReduce extends Reducer<Text, InfoBean, InfoBean, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();

            for (InfoBean value : values) {
                if ("1".equals(value.getFlag())) {
                    try {
                        BeanUtils.copyProperties(value, pdBean);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    InfoBean orderBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(orderBean, value);
                        orderBeans.add(orderBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // 拼接两类数据形成最终结果
            for (InfoBean bean : orderBeans) {

                bean.setpName(pdBean.getpName());
                bean.setCategoryId(pdBean.getCategoryId());
                bean.setPrice(pdBean.getPrice());

                context.write(bean, NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
//		job.setJar("c:/join.jar");

        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(TableJoinMapper.class);
        job.setReducerClass(TableJoinReduce.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("mr/data/join"));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("mr/data/join/result"));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /* job.submit(); */
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
