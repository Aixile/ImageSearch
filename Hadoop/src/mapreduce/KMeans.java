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
	private static int IterationLimit=6;
	//private static double THRESHOLD=70;
	private static int k=10;
	
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
	
	
	public static class ClusterMapper extends Mapper<LongWritable,BytesWritable,IntWritable,IntArrayWritable>{
		//private final static IntWritable one=new IntWritable(1);
		
		
		Vector<int[]> centers=new Vector<int[]>();
		@Override
		public void setup(Context context) throws IOException{

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
						int[] t=new int[130];
						for(int i=2;i<str.length;i++){
							t[i-2]=Integer.parseInt(str[i]);
						}
						centers.add(t);
					}
					br.close();
					if(centers.size()!=k){
						log.info("Center file not match:"+centers.size());
						System.exit(1);
					//	throw(new KMeansException("Center File Size Not match"));
					}
		}
		
		@Override
		public void map(LongWritable key,BytesWritable value,Context context) throws IOException, InterruptedException{
			log.info("Mapper start");
			byte[] b=value.getBytes();
			
			log.info(centers.size());
			int maxdist=Integer.MAX_VALUE;
			int index=-1;
			
			int[] ints=new int[128];
			IntWritable iw[]=new IntWritable[128];
			
			for(int i=0;i<128;i++){
				ints[i]=b[i+8]&0xFF;
				iw[i]=new IntWritable(ints[i]);
			}
			
			
			for(int i=0;i<k;i++){
				int ans=0;
				for(int j=0;j<128;j++){
					int q=centers.get(i)[j];
					
					if(q>ints[j])	ans+=q-ints[j];
					else ans+=ints[j]-q;
				}
				if(ans<maxdist){
					index=i;
					maxdist=ans;
				}
			}
			
			IntArrayWritable aw=new IntArrayWritable();
			aw.set(iw);
			context.write(new IntWritable(index),aw);
		}
		
	}
	
	public static class Combiner extends Reducer<IntWritable,IntArrayWritable,IntWritable,IntArrayWritable>{
		@Override
		public void reduce(IntWritable key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			log.info("Start Combiner");
			IntArrayWritable aw=new IntArrayWritable();
			IntWritable[] ints=new IntWritable[129];
			
			int[] ans=new int[129];
			
			int cnt=0;
			while(values.iterator().hasNext()){
				cnt++;
				Writable[] b=values.iterator().next().get();
				for(int i=0;i<128;i++){
					int bv=((IntWritable) b[i]).get();
					ans[i]+=bv;
				}
			}
			ans[128]=cnt;
			
			for(int i=0;i<129;i++)	ints[i]=new IntWritable(ans[i]);
			aw.set(ints);
			context.write(key, aw);
		}
	}
	
	
	
	public static class ClusterReducer extends Reducer<IntWritable,IntArrayWritable,IntWritable,Text>{
		@Override
		public void reduce(IntWritable key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			log.info("Start Reducer");
			int[] ans=new int[128];
			int cnt=0;
			while(values.iterator().hasNext()){
				Writable[] t=values.iterator().next().get();
				for(int i=0;i<128;i++)	ans[i]+=((IntWritable) t[i]).get();
				cnt+=((IntWritable) t[128]).get();
			}
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
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		Job job=Job.getInstance(conf,"Kmeans "+args[2]);
		
		if(args.length>=5) 	k=Integer.parseInt(args[4]);
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(KMeans.class);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(ClusterReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
	
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		CustomFixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFixedLengthInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		return job.waitForCompletion(true)?0:1;
		
	}

	public static Vector<int[]>  ReadCenter(Path cp,FileSystem fs) throws IOException{
		Vector<int[]> centers=new Vector<int[]>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(cp)));
		String line;
		while((line=br.readLine()) != null){
			String[] str = line.split("\\s+");
			int[] t=new int[129];
			for(int i=1;i<str.length;i++){
				t[i-1]=Integer.parseInt(str[i]);
			}
			centers.add(t);
		}
		br.close();
		return centers;
	}
	
	public static int CheckDistance(Configuration conf,String[] args) throws IOException{
		Path oldc=new Path(args[1]);
		Path newc=new Path(args[2]+"/part-r-00000");
		FileSystem fs=FileSystem.get(conf);
		if(!fs.exists(oldc)||!fs.exists(newc)){
			System.exit(1);
		}
		Vector<int[]> oldcenters= ReadCenter(oldc,fs);
		Vector<int[]> newcenters= ReadCenter(newc,fs);
		int ans=0;
		for(int i=0;i<k;i++){
			for(int j=1;j<129;j++){
				ans=ans+Math.abs(oldcenters.get(i)[j]-newcenters.get(i)[j]);
			}
			
		}
		return ans;
	}
	
	
	//public static int ChangeFile
	
	//arg[0] Input path
	//arg[1] Center file path
	//arg[2] Output path
	//arg[3] Iteration Limit
	//arg[4] K
	
	public static void main(String[] args) throws Exception{
		log.info("Start KMeans");
		
		if(args.length>=4)	IterationLimit=Integer.parseInt(args[3]);
		
		if(args.length>=5) 	k=Integer.parseInt(args[4]);
		
		Configuration conf=new Configuration();
		
		String str=new String(args[2]);
		
		//Job job=Job.getInstance(conf,"Kmeans Test");
		int success=1,cur=0;
		do{
			cur++;
			args[2]=str+String.valueOf(cur);
			log.info("Round "+String.valueOf(cur)+" starting");
			success^=ToolRunner.run(conf, new KMeans(),args);
			if(success==0){
				log.info("Failed in Round "+cur);
				break;
			}
			int dist=CheckDistance(conf, args);
			log.info("Round "+cur+ " Diff: "+dist);
			args[1]=args[2]+"/part-r-00000";
			
		}while(cur<IterationLimit);
	}
}
