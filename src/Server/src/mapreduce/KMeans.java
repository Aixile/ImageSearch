package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.DistributedCache;;

public class KMeans extends Configured implements Tool {
	//private static int IterationLimit=6;
	//private static double THRESHOLD=70;
	
	private static Log log=LogFactory.getLog(KMeans.class);
	
	
	public static class KMeansException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public KMeansException(String msg){
			super(msg);
		}
	}
	

	public static class ClusterMapper extends Mapper<LongWritable,BytesWritable,IntWritable,LongArrayWritable>{
		//private final static IntWritable one=new IntWritable(1);
		
		
		Vector<long[]> centers=new Vector<long[]>();
		int k;
		@Override
		public void setup(Context context) throws IOException{
					k=context.getConfiguration().getInt("k",10);
					
					log.info("Mapper setup start");
				//	Path[] cache=context.getCacheFiles();
					
					URI[] cache=context.getCacheFiles();
					if(cache == null || cache.length <=0) System.exit(1);
					log.info(cache[0].toString());
					Path cp=new Path(cache[0]);
					//log.info(cache[].toString());
					
					FileSystem fs=FileSystem.get(context.getConfiguration());
					
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(cp)));
					
					String line;
					while((line=br.readLine()) != null){
						String[] str = line.split("\\s+");
						long[] t=new long[130];
						for(int i=2;i<str.length;i++){
							t[i-2]=Long.parseLong(str[i]);
						}
						centers.add(t);
					}
					br.close();
					if(centers.size()!=k){
						log.info("Center file not match:"+centers.size()+" "+k);
						System.exit(1);
					//	throw(new KMeansException("Center File Size Not match"));
					}
		}
		
		@Override
		public void map(LongWritable key,BytesWritable value,Context context) throws IOException, InterruptedException{
			//log.info("Mapper start");
			byte[] b=value.getBytes();
			
			//log.info(centers.size());
			long maxdist=Long.MAX_VALUE;
			//int maxdist=Integer.MAX_VALUE;
			int index=-1;
			
			long[] ints=new long[129];
			LongWritable iw[]=new LongWritable[129];
			
			for(int i=0;i<128;i++){
				ints[i]=b[i+8]&0xFF;
				iw[i]=new LongWritable(ints[i]);
			}
			
			
			for(int i=0;i<k;i++){
				long ans=0;
				for(int j=0;j<128;j++){
					long q=centers.get(i)[j];
					
					if(q>ints[j])	ans+=q-ints[j];
					else ans+=ints[j]-q;
				}
				if(ans<maxdist){
					index=i;
					maxdist=ans;
				}
			}
			
			iw[128]=new LongWritable(1);
			
			LongArrayWritable aw=new LongArrayWritable();
			aw.set(iw);
			context.write(new IntWritable(index),aw);
		}
		
	}
	
	public static class Combiner extends Reducer<IntWritable,LongArrayWritable,IntWritable,LongArrayWritable>{
		@Override
		public void reduce(IntWritable key,Iterable<LongArrayWritable> values,Context context) throws IOException, InterruptedException{
			log.info("Start Combiner");
			LongArrayWritable aw=new LongArrayWritable();
			LongWritable[] ints=new LongWritable[129];
			
			long[] ans=new long[129];
			
			long cnt=0;
			while(values.iterator().hasNext()){
				Writable[] b=values.iterator().next().get();
				for(int i=0;i<128;i++){
					long bv=((LongWritable) b[i]).get();
					ans[i]+=bv;
				}
				cnt+=((LongWritable) b[128]).get();
			}
			//log.info("Count");
		//	log.info(cnt);
		//	log.info("Sum");
			//log.info(ans[0]);
			ans[128]=cnt;
			
			for(int i=0;i<129;i++)	ints[i]=new LongWritable(ans[i]);
			aw.set(ints);
			context.write(key, aw);
		}
	}
	
	
	
	public static class ClusterReducer extends Reducer<IntWritable,LongArrayWritable,IntWritable,Text>{
		@Override
		public void reduce(IntWritable key,Iterable<LongArrayWritable> values,Context context) throws IOException, InterruptedException{
			log.info("Start Reducer");
			long[] ans=new long[128];
			long cnt=0;
			while(values.iterator().hasNext()){
				Writable[] t=values.iterator().next().get();
				for(int i=0;i<128;i++)	ans[i]+=((LongWritable) t[i]).get();
				cnt+=((LongWritable) t[128]).get();
			}
			
			log.info("Count");
			log.info(cnt);
			log.info("Sum");
			log.info(ans[0]);
			
			
			
			for(int i=0;i<128;i++) ans[i]=ans[i]/cnt;

			
			String str=String.valueOf(ans[0]);
			for(int i=1;i<128;i++){
				str=str+" "+String.valueOf(ans[i]);
			}
			
			str=String.valueOf(cnt)+"\t"+str;
			context.write(key, new Text(str));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception{
	for(int i=0;i<args.length;i++) System.out.println(args[i]);
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		Job job=Job.getInstance(conf,"Kmeans "+args[2]);
		log.info(args);
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(KMeans.class);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(ClusterReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongArrayWritable.class);
	
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		CustomFixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFixedLengthInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		return job.waitForCompletion(true)?0:1;
		
	}

}
