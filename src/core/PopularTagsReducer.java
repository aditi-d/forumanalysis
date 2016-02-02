package core;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;

class Tag implements Comparable<Tag>{
	String tagName;
	int count;
	Tag() {}
	Tag(String name, int count) {
		this.tagName = name;
		this.count = count;
	}
	@Override
	public int compareTo(Tag o) {
		if(this.count > o.count) 
			return -1;
		else if(this.count < o.count)
			return 1;
		return 0;
	}
} 

public class PopularTagsReducer extends Reducer<Text, LongWritable, Text, IntWritable>{

	TreeSet<Tag> tagList = new TreeSet<Tag>();
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) {
		int count = 0;
		for(LongWritable value: values) {
			count++;
		}
		if(tagList.size() > 10) {
			Tag tag = (Tag)tagList.last();
			if(tag.count < count)
				tagList.remove(tag);
		}
		if(tagList.size() < 10)
			tagList.add(new Tag(key.toString(), count));
	}
	
	@Override
	protected void cleanup(Context context) {
		for(Tag t : tagList) {
			try {
				context.write(new Text(t.tagName), new IntWritable(t.count));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/*@Override
	public void reduce(Text key, Iterator<LongWritable> value, Context context)
			throws IOException {
		int count = 0;
		if(value != null) {
			while(value.hasNext()) {
				count++;
				value.next();
			}
			//output.collect(new Text(key), new IntWritable(count));
		}
	}*/
}
