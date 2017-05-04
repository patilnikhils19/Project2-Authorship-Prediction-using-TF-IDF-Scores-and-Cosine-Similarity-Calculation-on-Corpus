package knownAuthorship;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





	/**
	 * @author Nikhil Patil <patilnikhils19@gmail.com>
	 * Apr 6, 2017
	 * MainClass.java
	 */
	public class MainClass {
			
			private static final String OUT_PATH1="OUTPUTJOB1-WordCount";
			private static final String OUT_PATH2="OUTPUTJOB2-WordTf";
			private static final String OUT_PATH3="OUTPUTJOB3-WordNj";
			private static final String OUT_PATH4="OUTPUTJOB4-WordIDFTF";
		
			/**
			 * @param args
			 * @throws IOException
			 * @throws ClassNotFoundException
			 * @throws InterruptedException
			 */
			public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
					if (args.length != 2) {
							System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
							System.exit(-1);
							}
				//Unigram Calculation job will Start Here
					Configuration conf =new Configuration();
					Job job=Job.getInstance(conf);
					job.setJarByClass(MainClass.class);
					job.setMapperClass(WordMapper.class);
					job.setReducerClass(WordReducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);
					job.setInputFormatClass(TextInputFormat.class);
					job.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job, new Path(args[0]));
					FileOutputFormat.setOutputPath(job, new Path(OUT_PATH1));
					if (job.waitForCompletion(true)) System.out.println("Job One Completed");
					
				//Tf Calculation job Will Start Here
					Job job1=Job.getInstance(conf);
					job1.setJarByClass(MainClass.class);
					job1.setMapperClass(WordMapperTf.class);
					job1.setReducerClass(WordReducerTf.class);
					job1.setOutputKeyClass(Text.class);
					job1.setOutputValueClass(Text.class);
					job1.setInputFormatClass(TextInputFormat.class);
					job1.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job1, new Path(OUT_PATH1));
					FileOutputFormat.setOutputPath(job1, new Path(OUT_PATH2));
					if (job1.waitForCompletion(true)) System.out.println("Job Two Completed");
					
					Counters counters=job1.getCounters();
					Counter counter =counters.findCounter(knownAuthorship.WordReducerTf.TestCounters.TEST);
					long authorTotal=counter.getValue();	
					System.out.println("Author Total is"+authorTotal);
					conf.set("N",String.valueOf(authorTotal));
					
				//Idf Calculation job Will Start Here
					Job job2=Job.getInstance(conf);
					job2.setJarByClass(MainClass.class);
					job2.setMapperClass(WordMapperIdf.class);
					job2.setReducerClass(WordReducerIdf.class);
					job2.setOutputKeyClass(Text.class);
					job2.setOutputValueClass(Text.class);
					job2.setInputFormatClass(TextInputFormat.class);
					job2.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job2, new Path(OUT_PATH2));
					FileOutputFormat.setOutputPath(job2, new Path(OUT_PATH3));
					if (job2.waitForCompletion(true)) System.out.println("Job Three Completed");
					
				//VectorAttribute Calculation job Will Start Here
					Job job3=Job.getInstance(conf);
					job3.setJarByClass(MainClass.class);
					job3.setMapperClass(WordMapperTfIdf.class);
					job3.setReducerClass(WordReducerTfIdf.class);
					job3.setOutputKeyClass(Text.class);
					job3.setOutputValueClass(Text.class);
					job3.setInputFormatClass(TextInputFormat.class);
					job3.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job3, new Path(OUT_PATH3));
					FileOutputFormat.setOutputPath(job3, new Path(OUT_PATH4));
					if (job3.waitForCompletion(true)) System.out.println("Job Four Completed");
					
				//Word-Idf
					Job job4=Job.getInstance(conf);
					job4.setJarByClass(MainClass.class);
					job4.setMapperClass(WordIdfMap.class);
					job4.setReducerClass(WordIdfRed.class);
					job4.setOutputKeyClass(Text.class);
					job4.setOutputValueClass(Text.class);
					job4.setInputFormatClass(TextInputFormat.class);
					job4.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job4, new Path(OUT_PATH4));
					FileOutputFormat.setOutputPath(job4, new Path(args[1]));
					if (job4.waitForCompletion(true)) System.out.println("Job Five Completed"); 
					System.exit(job4.waitForCompletion(true) ? 0 : 1);


			}
}