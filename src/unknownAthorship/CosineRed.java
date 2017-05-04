package unknownAthorship;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author Nikhil Patil <patilnikhils19@gmail.com>
 * Apr 6, 2017
 * CosineRed.java
 */
public class CosineRed extends Reducer<Text,Text,Text,Text>{

	public static Map<String, String> UnknownA = new HashMap<String, String>();  //save words and their frequencies
	public static Map<String, String> knownA =new HashMap<String, String>();
	
	public static double sumation = 0,sumationA = 0,sumationB=0;
	public static double cosine=0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String line = null;

		
		if (UnknownA.isEmpty()){
			Path path = new Path(context.getConfiguration().get("unknown_author"));
			FileSystem f = FileSystem.get(new Configuration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(f.open(path)));
			//		            line = reader.readLine();
			while ((line = reader.readLine()) != null){
				String Data[] =  line.toString().split("\t");
				UnknownA.put(Data[0],(Data[1]+"\t"+Data[2]));
			}
			reader.close();
		} 

		//Map<String, String> knownA =new HashMap<String, String>();
		
		for(Text val: values) {
			String Value1 = val.toString();
			String[] ValueList = Value1.split("\t");
			knownA.put(ValueList[0],ValueList[1]);
		}      

		//float sumation = 0,sumationA = 0,sumationB=0;
		//float cosine=0;

		for(String Uword:UnknownA.keySet()){

			if (UnknownA.isEmpty()==false){
				double A =0, A1=0;
				double B =0, B1=0;

				if(knownA.containsKey(Uword)){		
					double T = Double.valueOf(knownA.get(Uword));
					String BS = UnknownA.get(Uword);
					String Bvalue[] = BS.split("\t");
					A = (double)(T*Double.valueOf(Bvalue[0]));
					B = Float.valueOf(Bvalue[1]);
					
					sumation = sumation + (double)(A*B);
					sumationA = (double) (sumationA + (double)(Math.pow(A, 2)));
					sumationB = (double) (sumationB + (double)(Math.pow(B, 2)));											
				}
				else
				{
					String BS1 = UnknownA.get(Uword);
					String Bvalue1[] = BS1.split("\t");
					B1 = Double.valueOf(Bvalue1[1]);
					A1 = (double) (0.5*Double.valueOf(Bvalue1[0]));
					
					sumation = sumation + (Double)(A1*B1);
					sumationA = (double) (sumationA + (double)(Math.pow(A1, 2)));
					sumationB = (double) (sumationB + (double)(Math.pow(B1, 2)));	
				}
				
			}
		}
		cosine =(double)((double)(sumation)/(double)(Math.sqrt(sumationA)*Math.sqrt(sumationB)));
		knownA.clear();
		context.write(new Text(key.toString()), new Text (String.valueOf(cosine)));
		sumation = 0; sumationA = 0; sumationB=0;
		cosine=0;
		
		
	} 
}
