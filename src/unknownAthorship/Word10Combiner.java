package unknownAthorship;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

	/**
	 * @author Nikhil Patil <patilnikhils19@gmail.com>
	 * Apr 6, 2017
	 * Word10Combiner.java
	 */
	public class Word10Combiner extends Reducer<Text, Text, Text, Text>{
		
		public static TreeMap < Double, String> TopValues = new TreeMap < Double,String >();
		public void reduce( Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

			for (Text value : values) 
			{
				String[] po =  value.toString().split("\t");
				TopValues.put(Double.valueOf(po[1]) ,po[0].toString());
				if (TopValues. size() > 10) {
					TopValues.remove(TopValues.firstKey());
				}
			}
			for (String t : TopValues.descendingMap().values()) {
				context.write(new Text(""),new Text(t));
			}
		}
		 
		

	       
	}
	