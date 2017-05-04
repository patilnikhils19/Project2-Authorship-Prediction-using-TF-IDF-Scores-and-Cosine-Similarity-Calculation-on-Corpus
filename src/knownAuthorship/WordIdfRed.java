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
		 * WordIdfRed.java
		 */
		public class WordIdfRed extends Reducer<Text,Text,Text,Text>{
				
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
						String idfword = null;			
						for(Text val: values) {
							String IValues = val.toString();
							String[] IdfValues = IValues.split("\t");
							idfword = IdfValues[0];
						}

						
						context.write(new Text(key.toString()), new Text (idfword));
						
				} 
}