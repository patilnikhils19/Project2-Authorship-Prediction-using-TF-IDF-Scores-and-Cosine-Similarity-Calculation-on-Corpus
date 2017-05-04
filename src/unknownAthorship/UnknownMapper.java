package unknownAthorship;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

/**
 * @author Nikhil Patil <patilnikhils19@gmail.com>
 * Apr 6, 2017
 * UnknownMapper.java
 */
public class UnknownMapper extends Mapper<LongWritable, Text, Text, Text>{


	public static Map<String,String> unknownaship=new HashMap<String,String>();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String line = null;
		if (unknownaship.isEmpty()){
			Path path = new Path(context.getConfiguration().get("AAV_author"));
			FileSystem f = FileSystem.get(new Configuration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(f.open(path)));
			//		            line = reader.readLine();
			while ((line = reader.readLine()) != null){
				String Data [] = line.toString().split("\t");						//Read The data from the cache file for unknown author
				String word = Data[1];												//Fetch the word from the cache file for unknown author
				String TermFreq = Data[2];											//Fetch the term frequency from cache file for unknown author
				unknownaship.put(word, TermFreq);									//Save the Word and Term Frequency for finding IDF for 
			}
		}


		float UTFIDF = (float) 0.0;
		String CurrentLine;
		CurrentLine = value.toString();											//Create String Containing One line
		String[] str = CurrentLine.split("\t");									//Split String with Given Delimiter 
		if(str.length > 1){
			String Words = str[0];  											//Separate Word
			String IDF = str[1];												//Separate Known Author IDF

			if(unknownaship.containsKey(Words)){								//Find the match word in line to get IDF
				float Ut = Float.valueOf(unknownaship.get(Words));
				float UIDF = Float.valueOf(IDF);								
				UTFIDF = (float)Ut*UIDF;												
			}
			else
			{
				UTFIDF = (float)(0.5*Float.valueOf(IDF));
			}

			Text OutKey = new Text (Words);										//Pass Author Last Name And Word as Key	
			Text OutValue = new Text (IDF+"\t"+UTFIDF);							//Pass Word And Count As Value
			context.write(OutKey, OutValue);

		}
	}
}