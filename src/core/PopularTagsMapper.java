package core;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

//https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/ch3/TopTenDriver.java
//https://hadoopi.wordpress.com/2013/05/27/understand-recordreader-inputsplit/

public class PopularTagsMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	//TreeMap topTen = new TreeMap<String, Integer>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		if(value != null) {
			String[] allValues = value.toString().split("\t");
			if(allValues.length >= 19) {
				String node_type = allValues[5];
				if(node_type.equals("question")) {
					String[] tags = allValues[2].split(" ");
					long question_id = Long.valueOf(allValues[0]);
					for(int i = 0; i < tags.length; i++) {
						try {
							context.write(new Text(tags[i]), new LongWritable(question_id));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//output.collect(new Text(tags[i]), new LongWritable(question_id));
					}
				}
			}
		}
	}
	
	/*@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
		if(value != null) {
			String[] allValues = value.toString().split("\t");
			if(allValues.length >= 19) {
				String node_type = allValues[5];
				if(node_type.equals("question")) {
					String[] tags = allValues[2].split(" ");
					long question_id = Long.valueOf(allValues[0]);
					for(int i = 0; i < tags.length; i++) {
						output.collect(new Text(tags[i]), new LongWritable(question_id));
					}
				}
			}
		}
	}*/
	
}
