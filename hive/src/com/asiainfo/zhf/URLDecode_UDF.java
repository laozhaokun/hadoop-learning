package com.asiainfo.zhf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 *@author:zhaohf@asiainfo.com
 *@date:2014年12月3日 下午2:27:38
 *@Description: TODO
 */
public class URLDecode_UDF extends UDF{
	public Text evaluate(final Text text){
		String str = "";
		try {
			str = URLDecoder.decode(text.toString(),"UTF-8");
			str = URLDecoder.decode(str,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(str);
	}

}
