package com.asiainfo.zhf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import eu.bitwalker.useragentutils.UserAgent;

public class ParseUserAgent_UDF extends UDF{
	public Text evaluate(final Text userAgent){
		StringBuilder builder = new StringBuilder();
		UserAgent ua = new UserAgent(userAgent.toString());
		builder.append(ua.getOperatingSystem()+"\t"+ua.getBrowser()+"\t"+ua.getBrowserVersion());
		return new Text(builder.toString());
	}
}
