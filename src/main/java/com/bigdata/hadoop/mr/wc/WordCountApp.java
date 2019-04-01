package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WordCountApp {

    public static void main(String[] args) throws IOException {
        // 创建一个Job
        Job job = Job.getInstance();
        // 设置Job对应的主类
        job.setJarByClass(WordCountApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

//        job.setMapOutputKeyClass(Text);
    }

}
