package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** 
 * @ClassName: TestMapSort 
 * @Description: TODO 
 * @author zhaohf@asiainfo.com 
 * @date 2014年8月29日 上午10:47:13 
 * 
 */
public class TestMapSort {
	public static void main(String[] args) {
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put("j2se", 20);
		map.put("j2ee", 10);
		map.put("j2me", 30);
		List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(
				map.entrySet());
		// 排序前
		for (int i = 0; i < infoIds.size(); i++) {
			String id = infoIds.get(i).toString();
			System.out.println(id);
		}
		// 排序
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});
		// 排序后
		for (int i = 0; i < infoIds.size(); i++) {
			String id = infoIds.get(i).toString();
			System.out.println(id);
		}
	}
}
