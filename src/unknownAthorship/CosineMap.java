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
	 * CosineMap.java
	 */
	public class CosineMap extends Mapper<LongWritable, Text, Text, Text>{
			
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String CurrentLine;
			CurrentLine = value.toString();											//Create String Containing One line
			
			String[] str = CurrentLine.split("\t");									//Split String with Given Delimiter 
			
			if(str.length > 2){
				
				
				String Author = str[0];												//Separate Author Name
				String Words = str[1];  											//Separate Word
				String Termfreq = str[2];											//Separate 
				String TermIdf = str[3];
				String TermfreqIdf = str[4];
				Text OutKey = new Text (Author);									//Pass Author Last Name And Word as Key	
				Text OutValue = new Text (Words+"\t"+Termfreq+"\t"+TermIdf+"\t"+TermfreqIdf);						//Pass Word And Count As Value
				context.write(OutKey, OutValue);

			}
		}
	}