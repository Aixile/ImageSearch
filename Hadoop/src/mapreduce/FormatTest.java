package mapreduce;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class FormatTest {

	public static class FormatTestMapper extends Mapper<LongWritable,BytesWritable,IntWritable,IntWritable>{
		private final static IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key,BytesWritable value,Context context) throws IOException, InterruptedException{

			byte[] b=value.getBytes();
			int len=value.getLength();
			for(int i=0;i<len;i++){
				context.write(new IntWritable(b[i]& 0xFF), one);
			}
			
		}
		
	}
	
	public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	

	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Format Test");
		job.setJarByClass(FormatTest.class);
		job.setMapperClass(FormatTestMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//CustomFixedLengthInputFormat.setRecordLength(conf, 128+8);
		CustomFixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFixedLengthInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
