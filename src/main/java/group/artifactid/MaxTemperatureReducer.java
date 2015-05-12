package group.artifactid;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.server.util.SerializeUtils;


public class MaxTemperatureReducer extends
		Reducer<Text, Text, Text, MapWritable> {
	@Override
	public void reduce(Text apmac, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
//		MapWritable apmacmap = new MapWritable();
		MapWritable clientmacmap = new MapWritable();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		
		// apmac clientmap
		for (Text value : values) {
			
			String subline = value.toString();
//			System. out. println(subline);
			// subline apmac1url2
			String clientmac = subline.substring(0, 10);
			int indexsplit=subline.indexOf("|");
			String url = subline.substring(10,indexsplit);
			System. out. println(clientmac);
			System. out. println(url);
			
			Text tclientmac = new Text(clientmac);
			Text turl = new Text(url);
				MapWritable urlmap = (MapWritable) clientmacmap.get(tclientmac);
				if(urlmap==null){
					urlmap=new MapWritable(); 
					clientmacmap.put(tclientmac,urlmap);
				}
				IntWritable eveurl = (IntWritable) urlmap.get(turl);
				if (eveurl == null) {
					urlmap.put(turl, new IntWritable(1));
				} else {
					urlmap.put(turl, new IntWritable(eveurl.get() + 1));
				}
		}
		
		
		
	
		
		
		clientmacmap.write(dataOut);
	  String str=	dataOut.toString();
		dataOut.close();
		System. out. println(str);
//		System. out. println(StringUtils.byteToHexString(SerializeUtils.serialize(clientmacmap)));
		context.write(apmac, clientmacmap);
	}
}

//
//
//
//public class MaxTemperatureReducer extends
//		Reducer<Text, Text, Text, ArrayWritable> {
//	@Override
//	public void reduce(Text apmac, Iterable<Text> values, Context context)
//			throws IOException, InterruptedException {
//		Map<String,String> map = new HashMap<String,String>();
//				
//		for (Text value : values) {
//			String subline = value.toString();
//			Text clientmac = new Text(subline.substring(0, 10));
//			int countindex=subline.indexOf("|");
//			Text url = new Text(subline.substring(10, countindex));
//			Text count=new Text(subline.substring(countindex));
//			String clientandurl=subline.substring(0, countindex);
//		String	 oldnum=map.get(clientandurl);
//			if(map.get(clientandurl)==null){
//				map.put(clientandurl, "1");
//			}else{
//				int numold=Integer.parseInt(oldnum);
//				map.put(clientandurl,(numold+1)+"");
//			}
//		}
//		context.write(apmac, clientmacmap);
//	}
//}