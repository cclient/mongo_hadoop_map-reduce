package group.artifactid;

import group.artifactid.MongoMaxTemperatureReducer.SortByCount;
import group.artifactid.MongoMaxTemperatureReducer.UrlCount;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class MongoMaxTemperatureCombine extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text apmac, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
//		System.out.print("combine\n");
		List<UrlCount> ls = new ArrayList<UrlCount>();
		Map<String, String> map = new HashMap<String, String>();
		for (Text value : values) {
			Object val = map.get(value);
			if (val == null) {
				map.put(value.toString(), "1");
			} else {
				int oldcount = Integer.parseInt(val.toString());
				map.put(value.toString(), (oldcount + 1) + "");
			}
		}
		for (Map.Entry<String, String> entry : map.entrySet()) {
			String newurl = entry.getKey() + "|" + entry.getValue();
//			System.out.print(newurl+"\n");
			context.write(apmac, new Text(newurl));
		}
	}
}
