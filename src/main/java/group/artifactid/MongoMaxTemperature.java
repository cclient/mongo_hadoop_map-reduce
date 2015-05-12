package group.artifactid;

//cc MaxTemperature Application to find the maximum temperature in the weather dataset
//vv MaxTemperature
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

public class MongoMaxTemperature extends MongoTool {
	public MongoMaxTemperature() {
		Configuration conf = new Configuration();
		MongoConfig config = new MongoConfig(conf);
		setConf(conf);
		MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
		MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		config.setInputURI("mongodb://172.16.231.1:27017/lewifi.auditOrigData");
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
//		if (args.length != 2) {
//			System.err
//					.println("Usage: MaxTemperature <input path> <output path>");
//			System.exit(-1);
//		}
//		Job job = new Job();
//		job.setJarByClass(MaxTemperature.class);
//		job.setJobName("Max temperature");
//
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setMapperClass(MaxTemperatureMapper.class);
//		// job.setReducerClass(MaxTemperatureReducer.class);
//
//		job.setReducerClass(MongoMaxTemperatureReducer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//
//		job.setOutputKeyClass(Text.class);
//		// job.setOutputValueClass(MapWritable.class);
//		job.setOutputValueClass(BSONWritable.class);
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		  System.exit(ToolRunner.run(new MongoMaxTemperature(), args));
	}
}
// ^^ MaxTemperature
