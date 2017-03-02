package my.mongoMapReduce;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.*;

public class MongoMaxTemperatureReducerCombine extends
		Reducer<Text, Text, Text, BSONWritable> {
	private List<Map.Entry<String,Integer>> compressTopList(BasicBSONObject topobj, int topnum) {
		List<Map.Entry<String,Integer>> studentList = new ArrayList<Map.Entry<String,Integer>>();
		for (Map.Entry<String, Object> entry : topobj.entrySet()) {
			String Url = entry.getKey();
			String scount = entry.getValue().toString();
			studentList.add(new AbstractMap.SimpleEntry<String,Integer>(Url, Integer.parseInt(scount)));
		}
		Collections.sort(studentList, new Comparator<Map.Entry<String,Integer>>() {
			@Override
			public int compare(Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		System.out.print("--------这里排序成功，但入库时，mongo按键名（）排序,这里的排序是为筛选前100条用\n");
		for (int i = 0; i < studentList.size(); i++) {
			System.out.print(studentList.get(i).getValue() + "\n");
		}
		if (studentList.size() > topnum) {
			studentList = studentList.subList(0, topnum);
		}
		return studentList;
	}
	private BasicBSONObject listToBSONObj(List<Map.Entry<String,Integer>> list){
		BasicBSONObject newurlcmap = new BasicBSONObject();
		for (Map.Entry<String, Integer> stringIntegerEntry : list) {
			newurlcmap.put(stringIntegerEntry.getKey(),stringIntegerEntry.getValue());
		}
		return newurlcmap;
	}

	@Override
	public void reduce(Text apmac, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		BasicBSONObject clientmacmap = new BasicBSONObject();
		int count = 0;
		for (Text value : values) {
			String subline = value.toString();
			String clientmac = subline.substring(0, 17);
			int indexcount = subline.indexOf("|");
			int maplastcount = 1;
			String url;
			if (indexcount > -1) {
				indexcount++;
				url = subline.substring(17, indexcount);
				String mapcount = subline.substring(indexcount);
				maplastcount = Integer.parseInt(mapcount);
			} else {
				url = subline.substring(17);
			}
			BasicBSONObject urlmap = (BasicBSONObject) clientmacmap
					.get(clientmac);
			if (urlmap == null) {
				urlmap = new BasicBSONObject();
				clientmacmap.put(clientmac, urlmap);
			}
			Object eveurl = urlmap.get(url);
			if (eveurl == null && !url.equals(" ")) {
				urlmap.put(url, maplastcount);
			} else {
				urlmap.put(url, Integer.parseInt(eveurl.toString())
						+ maplastcount);
			}
			count++;
			if (count == 10000) {
				urlmap = listToBSONObj(compressTopList(urlmap, 100));
			}
		}
		for (Map.Entry<String, Object> entry : clientmacmap.entrySet()) {
			BasicBSONObject urlmap = (BasicBSONObject) entry.getValue();
			urlmap = listToBSONObj(compressTopList(urlmap, 100));
		}
		context.write(apmac, new BSONWritable(clientmacmap));
	}
}