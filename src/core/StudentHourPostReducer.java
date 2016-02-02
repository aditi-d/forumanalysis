package core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StudentHourPostReducer extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{

	@Override
	public void reduce(LongWritable key, Iterator<IntWritable> values,
			OutputCollector<LongWritable, IntWritable> output, Reporter r)
			throws IOException {
		List<Integer> hourList = new ArrayList<Integer>();
		//System.out.println(values.hasNext() + "::" + values.next());
		if(values.hasNext()) {
			int val = values.next().get();
			hourList.add(val);
		}
		Collections.sort(hourList);
		//System.out.println(key + "::" + hourList.toString());
		Iterator<Integer> itr = hourList.listIterator();
		int oldVal = itr.hasNext() ? itr.next() : -1;
		int count = 1, maxCount = 0, maxCountHour = oldVal;
		while(itr.hasNext()) {
			int newVal = itr.next();
			if(newVal == oldVal) {
				count++;
			}
			else {
				oldVal = newVal;
				if(count > maxCount) {
					maxCount = count;
					maxCountHour = oldVal;
				}
				count = 0;
			}
		}
		if(oldVal != -1)
			output.collect(key, new IntWritable(maxCountHour));
	}
}
