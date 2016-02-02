package core;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class StudentHourPostMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable>{

	static Logger log = Logger.getLogger(LoggerInit.class.getName());
	
	@Override
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, IntWritable> output, Reporter r)
			throws IOException {
		//log.debug(values);
		if(values != null) {
			String[] allValues = values.toString().split("\t");
			//log.debug(allValues.length);
			Long val = -1L;
			Date date = null;
			int hour_of_day = 0;
			if (allValues.length >= 9) {
				/*long author_id = Long.valueOf(allValues[3]);
				// "2012-02-25 08:11:01.623548+00"
				DateFormat formatter = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSSSSS+OO");
				Date date = null;
				try {
					date = formatter.parse(allValues[8]);
					log.debug(date);
					Calendar calender = GregorianCalendar.getInstance();
					calender.setTime(date);
					int hour_of_day = calender.get(Calendar.HOUR_OF_DAY);
					output.collect(new LongWritable(author_id), new IntWritable(hour_of_day));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					log.debug("exception");
					e.printStackTrace();
				}*/
				//System.out.println(allValues[3] + "::" + allValues[4] + "::" + allValues[8] + "::" + allValues[9]);
				String author_id = allValues[3];
				String date_added = allValues[8];
				if(author_id.equals("\\N")) {
					author_id = allValues[8];
					date_added = allValues[9];
				}
				try {
					//System.out.println();
					String value = author_id.replaceAll("^\"|\"$", "");//.substring(1, (allValues[3].length()-2));
					val = Long.valueOf(value);
					//System.out.println();
					//System.out.println("less than 7::" + Arrays.toString(arr));
				}
				catch (NumberFormatException ex) {
					//System.out.println(arr[3] + "::exception");
				}
				try{
					DateFormat formatter = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSSSSS+SS");
					String trimmedVal = date_added.replaceAll("^\"|\"$", "");//.substring(1, (allValues[8].length()- 2));
					//System.out.println("date::" + allValues[8]);
					date = formatter.parse(trimmedVal);
					Calendar calender = GregorianCalendar.getInstance();
					calender.setTime(date);
					hour_of_day = calender.get(Calendar.HOUR_OF_DAY);
					//System.out.println(hour_of_day);
				}
				catch (ParseException ex) {
					//System.out.println("ex::" + date);
				}
				if(val != -1 && hour_of_day != -1 && date != null) {
					//System.out.println("Collected::" + val + "::" + hour_of_day);
					output.collect(new LongWritable(val), new IntWritable(hour_of_day));
				}
			}
		}	
	}
}
