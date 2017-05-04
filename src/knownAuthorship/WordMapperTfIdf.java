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
	 * WordMapperTfIdf.java
	 */
	public class WordMapperTfIdf extends Mapper<LongWritable, Text, Text, Text>{
			
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String CurrentLine;
			CurrentLine = value.toString();											//Create String Containing One line
			
			String[] str = CurrentLine.split("\t");									//Split String with Given Delimiter 
			
			if(str.length > 2){
				
				String Author = str[1];												//Separate Author Name
				String Words = str[0];  											//Separate Word
				String NoAuthorsUsed = str[2];												//Separate Term Frequency
				String TermF = str[3];												//Separate Term Frequency
				
				Configuration conf=context.getConfiguration();
				String authornumber=conf.get("N").toString();
				int N =Integer.parseInt(authornumber);
				float Idf=(float)(Math.log10((float)((float)(N)/(Float.valueOf(NoAuthorsUsed)))));
				float Tfidf= Float.valueOf(TermF)*Idf;
				
				Text OutKey = new Text (Author);													//Pass Author Last Name 	
				Text OutValue = new Text (Words+"\t"+TermF+"\t"+Idf+"\t"+Tfidf);					//Pass Word And Count As Value
				context.write(OutKey, OutValue);

			}
		}
	}