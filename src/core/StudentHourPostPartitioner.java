package core;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class StudentHourPostPartitioner implements Partitioner<LongWritable, IntWritable>{

	@Override
	public void configure(JobConf arg0) {
		
	}

	@Override
	public int getPartition(LongWritable key, IntWritable hour, int numOfTasks) {
		if(numOfTasks == 0)
			return 0;
		if(hour.get() >= 0 && hour.get() < 6) 
			return 0;
		else if(hour.get() >= 6 && hour.get() < 12)
			return 1 % numOfTasks;
		else if(hour.get() >= 12 && hour.get() < 18)
			return 2 % numOfTasks;
		else //if(hour.get() >= 18 && hour.get() < 24)
			return 3 % numOfTasks;
	}

	

}
