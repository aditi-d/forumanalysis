package core;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TestFileRead {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		FileReader fr = null;
		try {
			fr = new FileReader("input2//student_test_posts.csv");
			BufferedReader buffReader = new BufferedReader(fr);
			String line = null;
			try {
				while((line = buffReader.readLine()) != null) {
					String[] arr = line.split("\t");
					//System.out.println(arr.length);
					Long val = -1L;
					Date date = null;
					int hour_of_day = 0;
					int len = arr.length;
					System.out.println("====================================================");
					System.out.println("Length::" + len);
					for(int i = 0; i < len; i++) {
						System.out.println(arr[i].replaceAll("^\"|\"$", ""));
					}
					/*if(len == 15) {
						for(int i = 0; i < len; i++) {
							System.out.println(arr[i].replaceAll("^\"|\"$", ""));
						}
					}*/
					/*if(len == 19) 
						System.out.println(arr[5] + "::" + arr[6]);
					System.out.println("====================================================");*/
					/*if(arr.length >= 9) {
						try {
							//System.out.println();
							String value = arr[3].substring(1, (arr[3].length()-2));
							val = Long.valueOf(value);
							System.out.println();
							//System.out.println("less than 7::" + Arrays.toString(arr));
						}
						catch (NumberFormatException ex) {
							//System.out.println(arr[3] + "::exception");
						}
						try{
							DateFormat formatter = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSSSSS+SS");
							String trimmedVal = arr[8].substring(1, (arr[8].length()- 2));
							System.out.println("date::" + arr[8]);
							date = formatter.parse(trimmedVal);
							Calendar calender = GregorianCalendar.getInstance();
							calender.setTime(date);
							hour_of_day = calender.get(Calendar.HOUR_OF_DAY);
							System.out.println(hour_of_day);
						}
						catch (ParseException ex) {
							System.out.println("ex::" + date);
						}
						if(val != -1 && hour_of_day != -1)
						System.out.println(val + "::" + hour_of_day);
					}*/
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}

}
