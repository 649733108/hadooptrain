package com.imooc.hadoop.spring;
/*
 * Created by wxn
 * 2018/11/10 0:15
 */

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * 使用Spring Boot访问HDFS
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

	@Autowired
	FsShell fsShell;


	@Override
	public void run(String... strings) throws Exception {

		System.out.println("=========start===========");

		for (FileStatus fileStatus : fsShell.lsr("/springhdfs")) {
			System.out.println("> " + fileStatus.getPath());
		}

		System.out.println("=========end===========");

	}

	public static void main(String args[]) {
		SpringApplication.run(SpringBootHDFSApp.class ,args);
	}

}
