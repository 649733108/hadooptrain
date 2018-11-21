package com.imooc.hadoop.project;
/*
 * Created by wxn
 * 2018/11/5 21:56
 */


import com.amazonaws.transform.MapEntry;
import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentTest {

//	public static void main(String args[]) {
//
//		String source = "117.100.231.8 - - [10/Nov/2016:00:01:44 +0800] \"GET /favicon.ico HTTP/1.1\" 200 12118 \"www.imooc.com\" \"http://www.imooc.com/course/programdetail/pid/34\" - \"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.64:80 200 0.001 0.001";
//
//		UserAgentParser userAgentParser = new UserAgentParser();
//		UserAgent agent = userAgentParser.parse(source);
//
//	}

	@Test
	public void testReadFile() throws Exception {

		String path = "C:\\Users\\64973\\Desktop\\10000_access.log";

		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(path)))
		);

		String line = "";
		int i = 0;
		Map<String ,Integer> browserMap = new HashMap<>();
		UserAgentParser userAgentParser = new UserAgentParser();
		while (line != null) {
			line = reader.readLine();//一次读入一行数据
			i++;
			if (StringUtils.isNotBlank(line)) {
				String source = line.substring(getCharacterPosition(line, "\"", 7)+1);
				UserAgent agent = userAgentParser.parse(source);
				String browser = agent.getBrowser();
				String engine = agent.getEngine();
				String engineVersion = agent.getEngineVersion();
				String os = agent.getOs();
				String platform = agent.getPlatform();
				boolean isMobile = agent.isMobile();

				Integer browserValue = browserMap.get(browser);
				if (browserValue !=null){
					browserMap.put(browser, browserValue +1);
				}else {
					browserMap.put(browser,1);
				}

//				System.out.println(i + " : " + browser + " , " + engine + " , " + engineVersion + " , "
//						+ os + " , " + platform + " , " + isMobile);
			}
		}
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		for (Map.Entry<String ,Integer> entry : browserMap.entrySet()){
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}

	}

	@Test
	public void testGetCharacterPosition() {
		String value = "183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] \"POST /api3/getadv HTTP/1.1\" 200 813 \"www.imooc.com\" \"-\" cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96 \"mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G\" \"-\" 10.100.134.244:80 200 0.027 0.027";
		int index = getCharacterPosition(value, "\"", 1);
		System.out.println(index);
	}


	/**
	 * 获取指定字符串中指定标识的字符串出现的索引位置
	 */
	private int getCharacterPosition(String value, String operator, int index) {
		Matcher slashMatcher = Pattern.compile(operator).matcher(value);
		int mIdx = 0;
		while (slashMatcher.find()) {
			mIdx++;
			if (mIdx == index) {
				break;
			}
		}
		return slashMatcher.start();
	}


	@Test
	public void testUserAgentParser() {
		String source = "117.100.231.8 - - [10/Nov/2016:00:01:44 +0800] \"GET /favicon.ico HTTP/1.1\" 200 12118 \"www.imooc.com\" \"http://www.imooc.com/course/programdetail/pid/34\" - \"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.64:80 200 0.001 0.001";

		UserAgentParser userAgentParser = new UserAgentParser();
		UserAgent agent = userAgentParser.parse(source);
	}

}
