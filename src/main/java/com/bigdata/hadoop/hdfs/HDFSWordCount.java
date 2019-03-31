package com.bigdata.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class HDFSWordCount {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Path input = new Path("/hdfsapi/test/");
        // 配置远程hadoop服务端地址， 需要修改客户端的hosts文件
        URI uri = new URI("hdfs://zhege:8020");
        Configuration configuration = new Configuration();
        // 副本系数
        configuration.set("dfs.replication", "1");
        // 配置外网访问
        configuration.set("dfs.client.use.datanode.hostname","true");
        // 配置文件服务器地址
        configuration.set("fs.defaultFS", "hdfs://140.143.71.137:8020");
        // 需要指定hadoop所在用户
        FileSystem fs = FileSystem.get(uri, configuration, "hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        WordCountMapper wordCountMapper = new WordCountMapper();
        ImoocContext imoocContext = new ImoocContext();
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                wordCountMapper.map(line, imoocContext);
            }
            reader.close();
            in.close();
        }

        Map<Object, Object> contextMap = imoocContext.getCacheMap();
        Path output = new Path("/hdfsapi/output");
        FSDataOutputStream out = fs.create(new Path(output, new Path("wc.out")));
        contextMap.forEach((k, v) -> {
            try {
                out.write((k.toString() + "\t " + v.toString() + "\n").getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        out.flush();
        out.close();
        System.out.println("zhege的运行成功了");
    }

}
