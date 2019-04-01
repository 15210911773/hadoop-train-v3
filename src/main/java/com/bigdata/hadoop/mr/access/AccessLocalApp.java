package com.bigdata.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by jizhe.pan on 2019-04-01
 */
public class AccessLocalApp {

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
        job.setJarByClass(AccessLocalApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setPartitionerClass(AccessPartitioner.class);
        job.setNumReduceTasks(2);

        /**
         * Mapper输出key和value的类型
         */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);


        /**
         * Reducer输出key和value的类型
         */
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 提交job
        boolean result = job.waitForCompletion(true);

        // 退出job
        System.exit(result ? 0 : 1);
    }

}
