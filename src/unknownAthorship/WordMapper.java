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
	 * WordMapper.java
	 */
	public class WordMapper extends Mapper<LongWritable, Text, Text, Text>{
			
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String CurrentLine;
			CurrentLine = value.toString();											//Create String Containing One line
			
			String StrLower;
			StrLower = CurrentLine.toLowerCase();									//Convert String to Lower Case
			
			String[] str = StrLower.split("<===>");									//Split String with Given Delimiter 
			
			if(str.length > 2){
				
				String Author = str[0];												//Separate Author Name
				String Sentance = str[2];											//Separate Content Line
				Sentance = Sentance.replaceAll("[^a-zA-Z0-9 ]", "").trim();			//Remove 
				Sentance = Sentance.replaceAll("( )+", " ").trim();			
				Sentance = Sentance.replaceAll("/t", " ").trim();
				String[] AuthorLast = Author.split(" ");
				String AuthorLastName =AuthorLast[AuthorLast.length-1];				//Extract Authors Last name
				String[] Words = Sentance.split(" ");								//Split Sentence With Delimiter " "  
		
				if(Words.length != 0){																
					for(int i=0;i < Words.length;i++){
						if(Words[i].length()>=1){
							Text OutKey = new Text (AuthorLastName+"\t"+Words[i]);	//Pass Author Last Name And Word as Key	
							Text OutValue = new Text ("One");
							context.write(OutKey, OutValue);
							}
					}
				}
			}
		}
	}