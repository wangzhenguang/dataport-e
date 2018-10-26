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

/**
 * 查找共同好友
 * <p>
 * 第一步查找出 好友 分别在哪些人中
 * key(好友） value(有当前这个好友的人)
 */
public class FindMutualFriends {

    static class FindMutualFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] person_friends = value.toString().split(":");
            String person = person_friends[0];
            String friends = person_friends[1];

            for (String friend : friends.split(",")) {
                context.write(new Text(friend), new Text(person));
            }
        }

    }

    static class FindMutualFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                sb.append(value).append(",");
            }
            context.write(key, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(FindMutualFriends.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FindMutualFriendsMapper.class);
        job.setReducerClass(FindMutualFriendsReducer.class);

        FileInputFormat.setInputPaths(job, new Path("mr/data/friends"));
        FileOutputFormat.setOutputPath(job, new Path("mr/data/friends/out1"));

        job.waitForCompletion(true);

    }

}
