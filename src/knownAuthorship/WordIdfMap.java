package knownAuthorship;

import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
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
	 * WordIdfMap.java
	 */
	public class WordIdfMap extends Mapper<LongWritable, Text, Text, Text>{
			
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String CurrentLine;
			CurrentLine = value.toString();											//Create String Containing One line
			
			String[] str = CurrentLine.split("\t");									//Split String with Given Delimiter 
			
			if(str.length > 2){
				
				String Words = str[1];  											//Separate Word
				String Idf = str[3];												//Separate Idf
				Text OutKey = new Text (Words);										//Pass Author Word 	
				Text OutValue = new Text (Idf);										//Pass Idf
				context.write(OutKey, OutValue);

			}
		}
	}