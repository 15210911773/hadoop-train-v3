package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 通用模板, 配置Mapper Reducer的相关属性
 */
public class WordCountLocalApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        // 副本系数
//        configuration.set("dfs.replication", "1");
//        // 配置外网访问
//        configuration.set("dfs.client.use.datanode.hostname","true");
//        // 配置文件服务器地址
//        configuration.set("fs.defaultFS", "hdfs://zhege:8020");
        // 创建一个Job
        Job job = Job.getInstance(configuration);
        // 设置Job对应的主类
        job.setJarByClass(WordCountLocalApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        /**
         * Mapper输出key和value的类型
         */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        /**
         * Reducer输出key和value的类型
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 提交job
        boolean result = job.waitForCompletion(true);

        // 退出job
        System.exit(result ? 0 : 1);
    }

}
