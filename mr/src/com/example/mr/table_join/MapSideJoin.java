package com.example.mr.table_join;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * 只用map实现 表合并
 */
public class MapSideJoin {
    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> pdInfoMap = new HashMap<String, String>();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line;
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                String[] fields = line.split(",");
                pdInfoMap.put(fields[0], fields[1]);
            }

            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String orderLine = value.toString();
            String[] fields = orderLine.split("\t");
            String pdName = pdInfoMap.get(fields[1]);
            k.set(orderLine + "\t" + pdName);
            context.write(k, NullWritable.get());

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("mr/data/mapjoininput"));
        FileOutputFormat.setOutputPath(job, new Path("mr/data/mapjoininput/result"));
        job.addCacheFile(new URI("file:/Users/wzg/Desktop/android-project/dataport-e/mr/data/mapjoincache/pdts.txt"));

        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
