package my.mongoMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.util.ToolRunner;

public class MongoMaxTemperature extends MongoTool {
	public MongoMaxTemperature() {
		Configuration conf = new Configuration();
		MongoConfig config = new MongoConfig(conf);
		setConf(conf);
		MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
		MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		config.setInputURI("mongodb://172.16.231.1:27017/lewifi.auditOrigData");
		config.setQuery("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}");
		// config.setInputKey("headers.From");
		config.setMapper(MongoMaxTemperatureMapper.class);
		//Combiner
		config.setCombiner(MongoMaxTemperatureCombine.class);
//		config.setReducer(MongoMaxTemperatureReducer.class);
		config.setReducer(MongoMaxTemperatureReducerCombine.class);
		config.setMapperOutputKey(Text.class);
		config.setMapperOutputValue(Text.class);
		config.setOutputKey(Text.class);
		config.setOutputValue(BSONWritable.class);
		config.setOutputURI("mongodb://172.16.231.1:27017/lewifi.monthtopurlwithcombine");
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MongoMaxTemperature(), args));
	}
}
