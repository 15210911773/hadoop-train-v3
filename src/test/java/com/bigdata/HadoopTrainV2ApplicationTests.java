package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class HadoopTrainV2ApplicationTests {

	@Test
	public void contextLoads() {
	}

	/**
	 * 获取FileSystem
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
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
		return FileSystem.get(uri, configuration, "hadoop");
	}

	@Test
	public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
		Path path = new Path("/hdfsapi/test");
		boolean mkdirs = getFileSystem().mkdirs(path);
		System.out.println(mkdirs);
	}

	@Test
	public void copyFromLocal() throws InterruptedException, IOException, URISyntaxException {
		FileSystem fs = getFileSystem();
		Path src = new Path("D:\\test\\test.txt");
		Path dest = new Path("/hdfsapi/test");
		fs.copyFromLocalFile(src, dest);
	}

	@Test
	public void copyFromRemote() throws InterruptedException, IOException, URISyntaxException {
		FileSystem fs = getFileSystem();
		Path src = new Path("/hdfsapi/test/1.txt");
		Path dest = new Path("/test/1.txt");
		fs.copyToLocalFile(false, src, dest, true);
	}

	@Test
	public void createFile() throws InterruptedException, IOException, URISyntaxException {
		FileSystem fileSystem = getFileSystem();
		FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
		out.writeUTF("hello pk");
		out.writeUTF("world super");
		out.flush();
		out.close();
	}

	@Test
	public void listFiles() throws InterruptedException, IOException, URISyntaxException {
		FileSystem fs = getFileSystem();
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println("===============" + fileStatus.getPath());
		}
	}

}
