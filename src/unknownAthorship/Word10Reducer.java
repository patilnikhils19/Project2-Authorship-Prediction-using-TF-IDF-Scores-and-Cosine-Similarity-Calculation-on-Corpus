package unknownAthorship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author Nikhil Patil <patilnikhils19@gmail.com>
 * Apr 6, 2017
 * Word10Reducer.java
 */
public class Word10Reducer extends Reducer<Text,Text,Text,Text>{

	public static TreeMap < Double, String> Top = new TreeMap < Double,String >();
	public void reduce( Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

		for (Text value : values) 
		{
			String[] po =  value.toString().split("\t");
			Top.put(Double.valueOf(po[1]) ,po[0].toString());
			if (Top. size() > 10) {
				Top.remove(Top.firstKey());
			}
		}
		for (String t : Top.descendingMap().values()) {
			context.write(new Text(""),new Text(t));
		}
	}
}