package com.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by jizhe.pan on 2019-04-01
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     *
     * @param phone
     * @param access
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text phone, Access access, int i) {
        String s = phone.toString();
        if (s.startsWith("15")) {
            return 0;
        } else {
            return 1;
        }
    }

}
