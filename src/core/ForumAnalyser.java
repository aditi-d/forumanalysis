package core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;//org.apache.log4j.Logger

class MultiLineFormat extends TextInputFormat {
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return null;
		
	}
}

public class ForumAnalyser extends Configured implements Tool{
	
	static Logger log = Logger.getLogger(LoggerInit.class.getName());
	public static void main(String[] args) {
		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(new ForumAnalyser(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			System.exit(exitCode);
		}
	}

	public static void setStudentHourPostJob(JobConf conf) {
		FileInputFormat.setInputPaths(conf, new Path("input2"));
		FileOutputFormat.setOutputPath(conf, new Path("output_forum_post"));
		conf.setJarByClass(ForumAnalyser.class);
		
		conf.setMapperClass(StudentHourPostMapper.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		
		//conf.setPartitionerClass(StudentHourPostPartitioner.class);
		//conf.setNumReduceTasks(4);
		
		conf.setReducerClass(StudentHourPostReducer.class);	
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
	}
	
	public static void setAverageLengthJob(JobConf conf) {
		FileInputFormat.setInputPaths(conf, new Path("input2"));
		FileOutputFormat.setOutputPath(conf, new Path("output_average_length"));
		conf.setJarByClass(ForumAnalyser.class);
		
		conf.setMapperClass(AverageLengthMapper.class);
		conf.setReducerClass(AverageLengthReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
			
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
	}
	
	public static void setPopularTagsJob(Job conf) {
		try {
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(conf, new Path("input2"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(conf, new Path("output_popular_tags"));
		conf.setJarByClass(ForumAnalyser.class);
		
		conf.setMapperClass(PopularTagsMapper.class);
		conf.setReducerClass(PopularTagsReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
			
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "/");
		JobConf conf = new JobConf(ForumAnalyser.class); //Job.getInstance(this.getConf(), "forumanalyser");
		setStudentHourPostJob(conf);
		JobClient.runJob(conf);
		
		JobConf averageLengthConf = new JobConf(ForumAnalyser.class);
		setAverageLengthJob(averageLengthConf);
		JobClient.runJob(averageLengthConf);
		
		Configuration topTenTags = new Configuration();
		Job popularTagsJob = Job.getInstance(getConf(), "toptentags"); //new Job(topTenTags);
		setPopularTagsJob(popularTagsJob);
		return popularTagsJob.waitForCompletion(true) ? 0 : 1;
		//return 0;
	}
}
