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
		 * WordReducerIdf.java
		 */
		public class WordReducerIdf extends Reducer<Text,Text,Text,Text>{
				
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
						int AuthorCount = 0;
						Map<String, Double> SaveData = new HashMap<String, Double>();
						
						for(Text val: values) {
							AuthorCount++;
							String AuthorTf = val.toString();
							String[] AuthorTFList = AuthorTf.split("\t");
							SaveData.put(AuthorTFList[0], Double.valueOf(AuthorTFList[1]));
						}
						for(String Author: SaveData.keySet()) {
							context.write(new Text(key.toString()), new Text (Author+"\t"+AuthorCount+"\t"+String.valueOf(SaveData.get(Author))));
						}
				} 
}