package my.mongoMapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MongoMaxTemperatureCombine extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text apmac, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
			context.write(apmac, new Text(newurl));
		}
	}
}
