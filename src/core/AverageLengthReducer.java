package core;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AverageLengthReducer extends MapReduceBase implements Reducer<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void reduce(LongWritable key, Iterator<Text> value,
			OutputCollector<Text, DoubleWritable> output, Reporter report) throws IOException {
		int question_length = 0;
		float answer_length = 0;
		int answer_count = 0;
		while(value.hasNext()) {
			String entry = value.next().toString();
			String[] qa = entry.split("\t");
			if(qa.length == 2) {
				if(qa[0].equalsIgnoreCase("question")) {
					question_length = qa[1].length();
				}
				else {
					answer_length += qa[1].length();
					answer_count++;
				}
			}
		}
		float answer_avg = 0;
		if(answer_count != 0)
			answer_avg = (float)answer_length/answer_count;
		String finalText = key + "\t" + question_length;
		output.collect(new Text(finalText), new DoubleWritable(answer_avg));
	}
}
