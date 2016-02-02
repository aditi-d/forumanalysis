package core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class AverageLengthMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	static Logger log = Logger.getLogger(LoggerInit.class.getName());
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		if(value != null) {
			String[] allValues = value.toString().split("\t");
			log.debug("new entry::" + allValues[0]);
			if(allValues.length >= 19){
				String node_type = allValues[5];
				long question_id = -1;
				String body = null;
				if(node_type.equalsIgnoreCase("question")) {
					question_id = Long.valueOf(allValues[0].replaceAll("^\"|\"$", ""));
					body = "question	" + allValues[4].replaceAll("^\"|\"$", "");
				}
				else if(node_type.equalsIgnoreCase("answer")) {
					question_id = Long.valueOf(allValues[6].replaceAll("^\"|\"$", ""));
					body = "answer	" + allValues[4].replaceAll("^\"|\"$", "");
				}
				if(question_id != -1) {
					log.debug("map::" + question_id + "::" + body);
					output.collect(new LongWritable(question_id), new Text(body));
				}
			}	
		}
	}
}
