package com.example.mr.friend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/***
 *
 *
 * 上一步数据
 * 当前用户   当前用户和这些人是好友关系
 * A	I,K,C,B,G,F,H,O,D,
 * B	A,F,J,E,
 *
 * 遍历跟当前有关系的的用户
 * 两两组合
 * i-k a
 * i-c a
 *
 * 最终2个用户的共同好友会分配到一个reducer
 *
 */
public class FindMutualFriendsStep2 {

    static class FindMutualFriendsStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            String person = split[0];
            String[] friends = split[1].split(",");

            for (int i = 0; i < friends.length - 1; i++) {
                for (int j = i + 1; j < friends.length; j++) {
                    context.write(new Text(friends[i] + "-" + friends[j]), new Text(person));
                }
            }

        }
    }


    /**
     * 上一步数据
     * i-k  a
     * i-c a
     * ....
     */
    static class FindMutualFriendsStep2Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(FindMutualFriendsStep2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FindMutualFriendsStep2Mapper.class);
        job.setReducerClass(FindMutualFriendsStep2Reducer.class);

        FileInputFormat.setInputPaths(job, new Path("mr/data/friends/out1"));
        FileOutputFormat.setOutputPath(job, new Path("mr/data/friends/out2"));

        job.waitForCompletion(true);

    }
}
