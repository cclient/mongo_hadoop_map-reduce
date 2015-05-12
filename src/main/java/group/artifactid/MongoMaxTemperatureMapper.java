package group.artifactid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class MongoMaxTemperatureMapper extends
		Mapper<Object, BSONObject, Text, Text> {
	@Override
	public void map(final Object key, BSONObject val, Context context)
			throws IOException, InterruptedException {
		String apmac = (String) val.get("apMac");
		String clientmac = (String) val.get("clientMac");
		String url = (String) val.get("url");
		String proto = (String) val.get("proto");
		if (proto.equals("http")&&!url.equals("")) {
			if (url.indexOf("http://") == 0) {
				url = url.substring(7);
			}
			int firstargindex = url.indexOf('/');
			if(firstargindex>-1){
				url = url.substring(0, firstargindex);	
			}
			//验证输入 带.则参数错误，临时转为}
			url=url.replace('.','}');
			
			context.write(new Text(apmac), new Text(clientmac + url));
		}
	}
}
