package unknownAthorship;

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
		 * UnknownReducer.java
		 */
		public class UnknownReducer extends Reducer<Text,Text,Text,Text>{
				public float sumation = 0,sumationA = 0,sumationB=0;
				public double cosine=0;
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
					String tfidf = null, idf = null;			
					for(Text val: values) {
						String IVal = val.toString();
						String[] IdfVal = IVal.split("\t");
						idf = IdfVal[0];
						tfidf = IdfVal[1];
					}

					
					context.write(new Text(key.toString()), new Text (idf+"\t"+tfidf));
				} 
}