package group.artifactid;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {	
//		 System. out. println(key);
//		 System. out. println(value);
		String line = value.toString();
		String[] arr=line.split(" ");
		String apmac=arr[0];
		String clientmac=arr[1];
		String url=arr[2];
//		
//		 System. out. println(apmac);
//		 System. out. println(clientmac);
//		 System. out. println(url);
		//url 格式化
//		MapWritable  urlmap=new MapWritable();
//		MapWritable  clientmap=new MapWritable();
//		MapWritable  apmacmap=new MapWritable();
//		urlmap.put(new Text(url),new IntWritable(1));
//		clientmap.put(new Text(clientmac),urlmap);
//		apmacmap.put(new Text(apmac), clientmap);
		context.write(new Text(apmac), new Text(clientmac+url+"|1"));
	}
}
