package group.artifactid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

public class MongoMaxTemperatureReducerCombine extends
		Reducer<Text, Text, Text, BSONWritable> {
	public class UrlCount {
		public UrlCount(String url, int count) {
			this.Url = url;
			this.Count = count;
		}

		String Url;
		int Count;
	}

	public List<UrlCount> compresstopobj(BasicBSONObject topobj, int topnum) {
		List<UrlCount> studentList = new ArrayList<UrlCount>();
		for (Map.Entry<String, Object> entry : topobj.entrySet()) {
			String Url = entry.getKey();
			String scount = entry.getValue().toString();
			studentList.add(new UrlCount(Url, Integer.parseInt(scount)));
		}
		Collections.sort(studentList, new Comparator<UrlCount>() {
			@Override
			public int compare(UrlCount o1, UrlCount o2) {
				if (o1.Count > o2.Count) {
					return -1;
				} else if (o1.Count < o2.Count) {
					return 1;
				} else {
					return 0;
				}
			}
		});
		System.out.print("--------这里排序成功，但入库时，mongo按键名（）排序,这里的排序是为筛选前100条用\n");
		for (int i = 0; i < studentList.size(); i++) {
			System.out.print(studentList.get(i).Count + "\n");
		}

		if (studentList.size() > topnum) {
			studentList = studentList.subList(0, topnum);
		}
		return studentList;
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
			String url = null;
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
				List<UrlCount> arr = compresstopobj(urlmap, 100);
				BasicBSONObject newurlcmap = new BasicBSONObject();
				for (int i = 0; i < arr.size(); i++) {
					UrlCount cuc = arr.get(i);
					newurlcmap.put(cuc.Url, cuc.Count);
				}
				urlmap = newurlcmap;
			}
		}
		for (Map.Entry<String, Object> entry : clientmacmap.entrySet()) {
			BasicBSONObject urlmap = (BasicBSONObject) entry.getValue();
			List<UrlCount> arr = compresstopobj(urlmap, 100);
			BasicBSONObject newurlcmap = new BasicBSONObject();
			for (int i = 0; i < arr.size(); i++) {
				UrlCount cuc = arr.get(i);
				newurlcmap.put(cuc.Url, cuc.Count);
			}
			urlmap = newurlcmap;
		}
		context.write(apmac, new BSONWritable(clientmacmap));
	}
}