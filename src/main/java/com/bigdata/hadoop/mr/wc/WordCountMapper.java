package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * KEYIN
 * Map任务读数据的key的类型， offset， 每行数据起始位置的偏移量， Long
 * VALUEIN
 * Map任务读数据的value类型， 其实就是一行行的字符串， String

 * KEYOUT
 * map方法自定义实现输出的key的类型
 * VALUEOUT
 * map方法自定义输出的value的类型
 *
 * 词频统计 Map阶段
 *
 * Long、String、Integer...: java中的数据类型
 * Hadoop自定义数据类型： 分布式计算需要网络传输， 必须可以序列化和反序列化
 *  LongWritable(Long), Text(String), IntWritable(Integer)
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LoggerFactory.getLogger(WordCountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            IntWritable one = new IntWritable(1);
            context.write(new Text(word), one);
        }
    }
}
