package com.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jizhe.pan on 2019-04-01
 */
public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        for (Access value : values) {
            ups += value.getUp();
            downs += value.getDown();
        }
        context.write(NullWritable.get(), new Access(key.toString(), ups, downs));
    }
}