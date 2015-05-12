package group.artifactid;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.mongodb.hadoop.io.BSONWritable;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.bson.BasicBSONObject;

public class MongoMaxTemperatureReducer extends
		Reducer<Text, Text, Text, BSONWritable> {
	public class UrlCount {
		public UrlCount(String url, int count) {
			this.Url = url;
			this.Count = count;
		}

		String Url;
		int Count;
	}

	class SortByCount implements Comparator {
		public int compare(Object o1, Object o2) {
			UrlCount s1 = (UrlCount) o1;
			UrlCount s2 = (UrlCount) o2;
			if (s1.Count > s2.Count)
				return 1;
			return 0;
		}
	}

	public List<UrlCount> compresstopobj(BasicBSONObject topobj, int topnum) {
		List<UrlCount> studentList = new ArrayList<UrlCount>();
		for (Map.Entry<String, Object> entry : topobj.entrySet()) {
			String Url=entry.getKey();
			String scount=entry.getValue().toString();
			System.out.print(scount+"\n");
			studentList.add(new UrlCount(Url, Integer.parseInt(scount)));
		}
		Collections.sort(studentList, new SortByCount());
		if (studentList.size() > topnum) {
			studentList = studentList.subList(0, topnum);
		}
		return studentList;
	}

	//
	// var self=this;
	// var arr=[];
	// for(var key in topobj){
	// arr.push({url:key,count:topobj[key]});
	// }
	// arr=arr.sort(function(x,y){
	// return y.count-x.count;
	// })
	// if(arr.length>topnum){
	// arr.length=topnum;
	// }
	// return arr;
	// }

	@Override
	public void reduce(Text apmac, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		BasicBSONObject clientmacmap = new BasicBSONObject();
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		DataOutputStream dataOut = new DataOutputStream(out);
		// apmac clientmap
		System.out.print(apmac);
		int count = 0;
		for (Text value : values) {
			String subline = value.toString();
			String clientmac = subline.substring(0, 17);
			String url = subline.substring(17);
//			System.out.print(clientmac);
//			System.out.print(url);
			BasicBSONObject urlmap = (BasicBSONObject) clientmacmap
					.get(clientmac);
			if (urlmap == null) {
				urlmap = new BasicBSONObject();
				clientmacmap.put(clientmac, urlmap);
			}
			Object eveurl = urlmap.get(url);

			if (eveurl == null&&!url.equals(" ")) {
				urlmap.put(url, 1);
			} else {
				urlmap.put(url, Integer.parseInt(eveurl.toString()) + 1);
			}
			count++;
			if (count == 1000) {
				List<UrlCount> arr = compresstopobj(urlmap, 100);
				BasicBSONObject newurlcmap = new BasicBSONObject();
				for (int i = 0; i < arr.size(); i++) {
					UrlCount cuc = arr.get(i);
					newurlcmap.put(cuc.Url, cuc.Count);
				}
				urlmap=newurlcmap;
			}			
		}
//		System.out.print(clientmacmap);
		context.write(apmac, new BSONWritable(clientmacmap));
	}
}

//
//
//
// public class MaxTemperatureReducer extends
// Reducer<Text, Text, Text, ArrayWritable> {
// @Override
// public void reduce(Text apmac, Iterable<Text> values, Context context)
// throws IOException, InterruptedException {
// Map<String,String> map = new HashMap<String,String>();
//
// for (Text value : values) {
// String subline = value.toString();
// Text clientmac = new Text(subline.substring(0, 10));
// int countindex=subline.indexOf("|");
// Text url = new Text(subline.substring(10, countindex));
// Text count=new Text(subline.substring(countindex));
// String clientandurl=subline.substring(0, countindex);
// String oldnum=map.get(clientandurl);
// if(map.get(clientandurl)==null){
// map.put(clientandurl, "1");
// }else{
// int numold=Integer.parseInt(oldnum);
// map.put(clientandurl,(numold+1)+"");
// }
// }
// context.write(apmac, clientmacmap);
// }
// }