package unknownAthorship;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
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
			private static final String OUT_PATH3="OUTPUTJOB3-WordUaav";
			private static final String OUT_PATH4="OUTPUTJOB4-Acosine";
		
			
			public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
					if (args.length != 4) {
							System.out.printf("Usage: <jar file> <input dir> <WordIDf dir> <output dir>\n");
							System.exit(-1);
							}
				//Unigram Calculation job will Start Here
					Configuration conf =new Configuration();
					conf.set("unknown_author", "hdfs://nashville:30261" + "/user/nspatil/OUTPUTJOB3-WordUaav/part-r-00000" );
					conf.set("AAV_author", "hdfs://nashville:30261" + "/user/nspatil/OUTPUTJOB2-WordTf/part-r-00000" );
					Job job=Job.getInstance(conf);
					job.setJarByClass(MainClass.class);
					job.setMapperClass(WordMapper.class);
					job.setReducerClass(WordReducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);
					job.setInputFormatClass(TextInputFormat.class);
					job.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job, new Path(args[0])); // Unknown Author File
					FileOutputFormat.setOutputPath(job, new Path(OUT_PATH1));
					if (job.waitForCompletion(true)) System.out.println("Job One Completed ");
					
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
					FileOutputFormat.setOutputPath(job1, new Path(OUT_PATH2)); // Unknown Word Tf
					if (job1.waitForCompletion(true)) System.out.println("Job Two Completed");
					
					
				//Tf.Idf Calculation job Will Start Here
					Job job2=Job.getInstance(conf);
					job2.setJarByClass(MainClass.class);
					job2.setMapperClass(UnknownMapper.class);
					job2.setReducerClass(UnknownReducer.class);
					job2.setOutputKeyClass(Text.class);
					job2.setOutputValueClass(Text.class);
					job2.setInputFormatClass(TextInputFormat.class);
					job2.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job2, new Path(args[1]));  //WordIdf File
					FileOutputFormat.setOutputPath(job2, new Path(OUT_PATH3)); // Unknown Full Dimension
					if (job2.waitForCompletion(true)) System.out.println("Job Three Completed");
					
				//cosine Calculation job Will Start Here
					Job job3=Job.getInstance(conf);
					job3.setJarByClass(MainClass.class);
					job3.setMapperClass(CosineMap.class);
					job3.setReducerClass(CosineRed.class);
					job3.setOutputKeyClass(Text.class);
					job3.setOutputValueClass(Text.class);
					job3.setInputFormatClass(TextInputFormat.class);
					job3.setOutputFormatClass(TextOutputFormat.class);
					FileInputFormat.setInputPaths(job3, new Path(args[2])); //Known TFIDF file
					FileOutputFormat.setOutputPath(job3, new Path(OUT_PATH4)); //Cosine Output
					if (job3.waitForCompletion(true)) System.out.println("Job four Completed ");
				
					System.exit(job3.waitForCompletion(true) ? 0 : 1);
			}
}