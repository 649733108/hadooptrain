package com.imooc.hadoop.spring;
/*
 * Created by wxn
 * 2018/11/9 23:38
 */

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 使用Spring Hadoop来访问HDFS文件系统
 */
public class SpringHadoopHDFSApp {

	private ApplicationContext ctx;
	private FileSystem fileSystem;

	@Test
	public void testMkdir()throws Exception{
		fileSystem.mkdirs(new Path("/springhdfs"));
	}

	@Test
	public void testShow()throws Exception{
		FSDataInputStream in = fileSystem.open(new Path("/springhdfs/hello.txt"));
		IOUtils.copyBytes(in,System.out,1024);
		in.close();
	}

	@Test
	public void testListPath()throws Exception{
		FileStatus[] fileStatus = fileSystem.listStatus(new Path("/springhdfs"));
		for (FileStatus fileStatus1 : fileStatus){
			System.out.println("> "+fileStatus1.getPath());
		}
	}

	@Before
	public void setUp(){
		ctx = new ClassPathXmlApplicationContext("beans.xml");
		fileSystem = (FileSystem) ctx.getBean("fileSystem");
	}

	@After
	public void tearDown(){
		ctx = null;
	}
}
