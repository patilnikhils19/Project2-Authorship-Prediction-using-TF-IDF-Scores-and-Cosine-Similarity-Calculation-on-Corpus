package knownAuthorship;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
		
		/**
		 * @author Nikhil Patil <patilnikhils19@gmail.com>
		 * Apr 6, 2017
		 * WordReducer.java
		 */
		public class WordReducer extends Reducer<Text,Text,Text,IntWritable>{
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
						int count = 0;
						for(Text val: values) {
							count++;
						}
						context.write(key, new IntWritable(count));	
						
				}
}