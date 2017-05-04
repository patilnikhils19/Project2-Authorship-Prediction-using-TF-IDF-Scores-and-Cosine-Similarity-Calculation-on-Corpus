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
		 * WordReducerTfIdf.java
		 */
		public class WordReducerTfIdf extends Reducer<Text,Text,Text,Text>{
				
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
						Map<String, Double> Attribute = new HashMap<String, Double>();						
						for(Text val: values) {
							String Values = val.toString();
							String[] AttributeValues = Values.split("\t");
							Attribute.put(AttributeValues[0]+"\t"+AttributeValues[1]+"\t"+AttributeValues[2]+"\t"+AttributeValues[3], 1.0);
						}
						for(String AttributeVector : Attribute.keySet()) {
							context.write(new Text(key.toString()), new Text (AttributeVector));
						}
				} 
}