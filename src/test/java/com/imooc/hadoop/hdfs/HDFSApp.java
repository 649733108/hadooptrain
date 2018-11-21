package com.imooc.hadoop.hdfs;
/*
 * Created by wxn
 * 2018/11/3 23:08
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Hadoop HDFS Java API操作
 */
public class HDFSApp {

	public static final String HDFS_PATH = "hdfs://192.168.26.128:8020";

	FileSystem fileSystem = null;
	Configuration configuration = null;

	/**
	 * 创建HDFS目录
	 */
	@Test
	public void mkdir() throws Exception {
		fileSystem.mkdirs(new Path("/hdfsapi/test1"));
	}

	/**
	 * 创建文件
	 */
	@Test
	public void create() throws IOException {
		FSDataOutputStream output = fileSystem.create(new Path("/a.txt"));
		output.write("".getBytes());
		output.flush();
		output.close();
	}

	/**
	 * 查看hdfs文件内容
	 */
	@Test
	public void cat() throws Exception{
		FSDataInputStream input = fileSystem.open(new Path("/hello.txt"));
		IOUtils.copyBytes(input,System.out,1024);
		input.close();
	}

	/**
	 * 重命名
	 * @throws Exception
	 */
	@Test
	public void rename()throws Exception{
		Path oldPath = new Path("/hdfsapi/test/a.txt");
		Path newPath = new Path("/hdfsapi/test/c.txt");
		fileSystem.rename(oldPath,newPath);
	}

	/**
	 * 上传文件到hdfs
	 * @throws Exception
	 */
	@Test
	public void copyFromLocalFile()throws Exception{
		Path localPath = new Path("C:\\Users\\64973\\Pictures\\Camera Roll\\qrcode_for_gh_32381e38ff33_258.jpg");
		Path hdfsPath = new Path("/hdfsapi/test");
		fileSystem.copyFromLocalFile(localPath,hdfsPath);
	}

	/**
	 * 上传文件到hdfs带进度条
	 * @throws Exception
	 */
	@Test
	public void copyFromLocalFileWithProgress()throws Exception{

		InputStream in = new BufferedInputStream(new FileInputStream(
				new File("C:\\Users\\64973\\Downloads\\jdk-8u191-linux-x64.tar.gz")
		));
		FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/jdk.tar.gz"), new Progressable() {
			@Override
			public void progress() {
				System.out.print(".");//带进度提醒信息
			}
		});
		IOUtils.copyBytes(in,output,4096);

	}

	/**
	 * 下载HDFS文件
	 */
	@Test
	public void copyToLocalFile()throws Exception{
		Path localPath = new Path("C:/Users/64973/Desktop/");
		Path hdfsPath = new Path("/hdfsapi/test/b.txt");

		/*
		src文件位于此文件系统下，dst位于本地磁盘上。将其从远程文件系统复制到本地dst名称。delSrc指示src是否会被删除。
		useRawLocalFileSystem指示是否使用RawLocalFileSystem作为本地文件系统。
		RawLocalFileSystem不是校验和，所以它不会在本地创建任何crc文件。

		delSrc : 是否删除src
		useRawLocalFileSystem：是否使用RawLocalFileSystem作为本地文件系统。
		 */
		fileSystem.copyToLocalFile(false,hdfsPath,localPath,true);
	}

	/**
	 * 查看某个目录下的所有文件
	 */
	@Test
	public void listFiles()throws Exception{
		FileStatus[] fileStatues = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : fileStatues){
			String isDir = fileStatus.isDirectory()?"文件夹":"文件";
			short replication = fileStatus.getReplication();
			long len = fileStatus.getLen();
			String path = fileStatus.getPath().toString();

			System.out.println(isDir + "\t" + replication + "\t" + len +"\t" + path);

		}
	}

	/**
	 * 删除
	 */
	@Test
	public void delete()throws Exception{
		fileSystem.delete(new Path("/hdfsapi/test/"),true);
	}

	@Before
	public void setUp() throws Exception {

		configuration = new Configuration();
		fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration,"hadoop");
		System.out.println("HDFSApp.setUp");
	}

	@After
	public void tearDown() throws Exception {
		configuration = null;
		fileSystem = null;

		System.out.println("HDFSApp.tearDown");
	}
}
