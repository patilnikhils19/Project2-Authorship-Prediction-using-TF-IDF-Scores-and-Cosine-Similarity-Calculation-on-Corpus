
package unknownAthorship;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * @author Nikhil Patil <patilnikhils19@gmail.com>
 * Apr 6, 2017
 * Word10Map.java
 */
public class Word10Map extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{
		
		context.write(new Text(""),new Text(value));
		
	}
	
}
