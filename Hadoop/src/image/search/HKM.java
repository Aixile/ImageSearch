package image.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class HKM extends Configured implements Tool {
	private static int IterationLimit=6;
	private static int MaxDepth=3;
	//private static double THRESHOLD=70;
	private static int k=10;
	public static int level=0;
	private static int itr=0;
	public static String OutputPrefix="";
	private static String CenterPrefix="";
	
	private static Log log=LogFactory.getLog(HKM.class);
	public static Boolean ReachLeaf=false;
	
	public static class KMeansException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public KMeansException(String msg){
			super(msg);
		}
	}
	
	
	public static class ClusterMapper extends Mapper<LongWritable,Text,IntWritable,IntArrayWritable>{
		//private final static IntWritable one=new IntWritable(1);
		
	//	Vector<byte[]> centers=new Vector<byte[]>(); 
		Vector<int[]> centers=new Vector<int[]>();
		@Override
		public void setup(Context context) throws IOException{

					log.info("Mapper setup start");
	
					URI[] cache=context.getCacheFiles();
					if(cache == null || cache.length <=0) System.exit(1);
					Path cp=new Path(cache[0]);
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
					int ans=k;
					for(int i=0;i<level;i++) ans*=k;
					br.close();
					if(centers.size()!=ans){
						log.info("Center file not match:"+centers.size());
						log.info("Center file expect:"+ans);
						System.exit(1);
					//	throw(new KMeansException("Center File Size Not match"));
					}
		}
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			log.info("Mapper start");
			
			log.info(centers.size());
			int maxdist=Integer.MAX_VALUE;
			int index=-1;
			
			int[] ints=new int[128];
			IntWritable iw[]=new IntWritable[128];
			
			
			String[] str=value.toString().split("\\s+");
			
			
			for(int i=0;i<128;i++){
				ints[i]=Integer.parseInt(str[i+3]);
				iw[i]=new IntWritable(ints[i]);
			}
			
			
			int pos=Integer.parseInt(str[0]);
			
			
			for(int i=pos*k;i<(pos+1)*k;i++){
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
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(HKM.class);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(ClusterReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
	
		int cc=k;
		for(int t=0;t<level-1;t++){
			cc*=k;
		}	
		String o=OutputPrefix+"/indexd"+String.valueOf(level-1);
		for(int i=0;i<cc;i++){
			TextInputFormat.addInputPath(job, new Path(o+"/index"+String.valueOf(i)+"-r-00000"));
		}
		job.setInputFormatClass(TextInputFormat.class);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
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
	
	
	
	public static void GenNewCenterFile(Configuration conf,String[] args) throws IOException{
		Path centerPath=new Path(CenterPrefix+String.valueOf(level));
		FileSystem fs=FileSystem.get(conf);
		int cc=k;
		for(int t=0;t<level-1;t++){
			cc*=k;
		}
		
		String o=OutputPrefix+"/indexd"+String.valueOf(level-1);
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(centerPath,true)));
		for(int i=0;i<cc;i++){
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(o+"/index"+String.valueOf(i)+"-r-00000"))));
			String line;
			int cnt=0;
			while((line=br.readLine()) != null){
				
				String[] str = line.split("\\s+");
				String ansStr=String.valueOf(i*k+cnt)+"\t0\t"+str[3];
				
				for(int j=4;j<str.length;j++){
					ansStr+=" "+str[j];
				}
				bw.write(ansStr+"\n");
				cnt++;
				if(cnt==k)	break;
			}
			br.close();
		}
		bw.close();
		//String centerDir=OutputPrefix+"/indexd"+String.valueOf(level-1);
	}
	
	
	//public static int ChangeFile
	
	//arg[0] Input path
	//arg[1] Center file path
	//arg[2] Output path
	//arg[3] Iteration Limit
	//arg[4] K
	//arg[5] MaxDepth
	
	
	public static void main(String[] args) throws Exception{
		log.info("Start HKM");
		
		
		if(args.length>=4)	IterationLimit=Integer.parseInt(args[3]);
		
		if(args.length>=5) 	k=Integer.parseInt(args[4]);
		
		if(args.length>=6) MaxDepth=Integer.parseInt(args[5]);
		
		
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		
		OutputPrefix=new String(args[2]);
		CenterPrefix=new String(args[1]);
		
		int success=1;
		for(level=0;level<MaxDepth;level++){
			if(ReachLeaf) break;
			if(level==0){
				itr=0;
				
				args[1]=CenterPrefix+String.valueOf(level);
				
				do{
					itr++;
					args[2]=OutputPrefix+"/d"+String.valueOf(level)+"r"+String.valueOf(itr);
					log.info("Round "+String.valueOf(itr)+" starting");
					success^=ToolRunner.run(conf, new KMeans(),args);
					if(success==0){
						log.info("Failed in Round "+itr);
						break;
					}
					
					int dist=CheckDistance(conf, args);
					log.info("Round "+itr+ " Diff: "+dist);
					if(itr>1)	fs.delete(new Path(args[1]),true);
					args[1]=args[2]+"/part-r-00000";
					
				}while(itr<IterationLimit);
				if(success==1){
					args[2]=OutputPrefix+"/indexd"+String.valueOf(level);
					ToolRunner.run(conf, new BuildIndex(),args);
				}
			}else{
				GenNewCenterFile(conf,args);
				itr=0;
				args[1]=CenterPrefix+String.valueOf(level);
				do{
					itr++;
					args[2]=OutputPrefix+"/d"+String.valueOf(level)+"r"+String.valueOf(itr);
					log.info("Round "+String.valueOf(itr)+" starting");
					success^=ToolRunner.run(conf, new HKM(),args);
					if(success==0){
						log.info("Failed in Round "+itr);
						break;
					}
					
					int dist=CheckDistance(conf, args);
					log.info("Round "+itr+ " Diff: "+dist);
					if(itr>1)	fs.delete(new Path(args[1]),true);
					args[1]=args[2]+"/part-r-00000";
					
				}while(itr<IterationLimit);
				if(success==1){
					args[2]=OutputPrefix+"/indexd"+String.valueOf(level);
					args[3]=String.valueOf(level);
					ToolRunner.run(conf, new BuildIndex2(),args);
				}
			}
		}
	}
	

}
