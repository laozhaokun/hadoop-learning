package test;


/**
 * @ClassName: SplitTest
 * @Description: TODO
 * @author zhaohf@asiainfo.com
 * @date 2014年8月27日 下午2:38:41
 * 
 */
public class SplitTest {
	public static void main(String[] args) {
		String str = "2063|2074|2075|2076|2080";
		String[] s = str.split("\\|");
		for(String i:s)
			System.out.println(i);
	}
}
