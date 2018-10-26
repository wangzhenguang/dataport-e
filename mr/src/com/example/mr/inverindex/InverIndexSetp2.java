package com.example.mr.inverindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 计算出 目录下 单个文件中 单词出现的总数
 * 上一步数据
 * hello-a.txt	3
 * hello-b.txt	2
 * hello-c.txt	2
 * jerry-a.txt	1
 * jerry-b.txt	3
 */
public class InverIndexSetp2 {

    static class InverIndexSetp2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String word = line.split("-")[0];
            String fileName_count = line.split("-")[1];
            k.set(word);
            v.set(fileName_count);
            context.write(k, v);

        }
    }

    static class InverIndexSetp2Reducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 接收的数据
         * word filename_count
         *
         * 将同一个单词出现过的文件名 和 总数都拼起来
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb .append(value).append(",");
            }

            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(InverIndexSetp2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("mr/data/inverindexinput/out1"));
        FileOutputFormat.setOutputPath(job, new Path("mr/data/inverindexinput/out2"));

        job.setMapperClass(InverIndexSetp2Mapper.class);
        job.setReducerClass(InverIndexSetp2Reducer.class);

        job.waitForCompletion(true);
    }


}
