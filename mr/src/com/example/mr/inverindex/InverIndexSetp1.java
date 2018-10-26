package com.example.mr.inverindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 计算出 目录下 单个文件中 单词出现的总数
 */
public class InverIndexSetp1 {

    static class InverIndexSetp1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            /**
             * word-filename 1
             */
            for (String word : words) {
                k.set(word + "-" + fileName);
                context.write(k, v);
            }

        }
    }

    static class InverIndexSetp1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * 接收的数据
         * word-filename 1
         * <p>
         * 处理数据
         * word-filename count(累加的数）
         * <p>
         * 后面接着处理
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(InverIndexSetp1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("mr/data/inverindexinput"));
        FileOutputFormat.setOutputPath(job, new Path("mr/data/inverindexinput/out1"));

        job.setMapperClass(InverIndexSetp1Mapper.class);
        job.setReducerClass(InverIndexSetp1Reducer.class);

        job.waitForCompletion(true);
    }


}
