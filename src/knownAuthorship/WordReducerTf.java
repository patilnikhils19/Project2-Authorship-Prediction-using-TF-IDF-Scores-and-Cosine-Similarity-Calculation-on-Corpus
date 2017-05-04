package knownAuthorship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
		
		/**
		 * @author Nikhil Patil <patilnikhils19@gmail.com>
		 * Apr 6, 2017
		 * WordReducerTf.java
		 */
		public class WordReducerTf extends Reducer<Text,Text,Text,Text>{
				public static enum TestCounters { TEST }
				//public int AuthorCount = 0;
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
						float maximumValue = 0;
						Map<String, Integer> SaveKeyValue = new HashMap<String, Integer>();  //save words and their frequencies
						Map<String,Integer>AuthorCount=new HashMap<String,Integer>();
						for(Text val: values) {
							String WordCount = val.toString();
							String[] Words = WordCount.split("\t");
							SaveKeyValue.put(Words[0], Integer.valueOf(Words[1]));
							float count = Float.parseFloat(Words[1]);
							if(maximumValue<count){
								maximumValue = count;
							}	
						}
						//AuthorCount++;
						if(!AuthorCount.containsKey(key))
						{
							AuthorCount.put(key.toString(), 1);
							
							//Using Counters
							context.getCounter(TestCounters.TEST).increment(1);
						}
						
						for(String Word1: SaveKeyValue.keySet()) {
							float termfrequency = (float) (0.5+0.5*(SaveKeyValue.get(Word1)/maximumValue));
							context.write(new Text(key.toString()), new Text (Word1+"\t"+termfrequency));
						}
				} 
}