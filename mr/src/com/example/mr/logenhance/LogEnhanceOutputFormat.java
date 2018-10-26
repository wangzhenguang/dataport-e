package com.example.mr.logenhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        Path path = new Path("mr/data/logEnhance/log.dat");
        Path tocrawlPath = new Path("mr/data/logEnhance/url.dat"); //待分析日志文件

        return new EnhanceRecordWriter(fileSystem.create(path), fileSystem.create(tocrawlPath));
    }

    static class EnhanceRecordWriter extends RecordWriter<Text, NullWritable> {

        FSDataOutputStream enhancedOs;
        FSDataOutputStream tocrawlOs;

        public EnhanceRecordWriter(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs) {
            super();
            this.enhancedOs = enhancedOs;
            this.tocrawlOs = tocrawlOs;
        }


        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String s = text.toString();
            if (s.contains("tocrawl")) {
                tocrawlOs.write(s.getBytes());
            } else {
                enhancedOs.write(s.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (tocrawlOs != null) {
                tocrawlOs.close();
            }
            if (enhancedOs != null) {
                enhancedOs.close();
            }
        }
    }
}
