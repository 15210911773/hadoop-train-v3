package com.bigdata.hadoop.hdfs;

public class WordCountMapper implements ImoocMapper {
    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Integer value = (Integer) context.getCacheMap().get(word);
            if (value != null) {
                context.getCacheMap().put(word, ++value);
            } else {
                context.getCacheMap().put(word, 1);
            }
        }
    }
}
