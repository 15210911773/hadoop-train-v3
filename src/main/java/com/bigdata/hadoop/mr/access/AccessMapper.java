package com.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jizhe.pan on 2019-04-01
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        String phone = words[0];
        long up = Long.parseLong(words[1]);
        long down = Long.parseLong(words[2]);

        context.write(new Text(phone), new Access(phone, up, down));
    }
}
