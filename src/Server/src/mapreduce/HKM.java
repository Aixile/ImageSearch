package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
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
	//public static int level=0;
	private static int itr=0;
	private static String OutputPrefix="";
	private static String CenterPrefix="";
	
	private static Log log=LogFactory.getLog(HKM.class);
	//public static Boolean ReachLeaf=false;
	
	public static class KMeansException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public KMeansException(String msg){
			super(msg);
		}
	}
	
	
	public static class ClusterMapper extends Mapper<LongWritable,Text,IntWritable,LongArrayWritable>{
		//private final static IntWritable one=new IntWritable(1);
		
	//	Vector<byte[]> centers=new Vector<byte[]>(); 
		Vector<long[]> centers=new Vector<long[]>();
		int k;
		int level;
		@Override
		public void setup(Context context) throws IOException{

					log.info("Mapper setup start");
					k=context.getConfiguration().getInt("k",10);
					level=context.getConfiguration().getInt("level", 0);
					
					URI[] cache=context.getCacheFiles();
					if(cache == null || cache.length <=0) System.exit(1);
					Path cp=new Path(cache[0]);
					FileSystem fs=FileSystem.get(context.getConfiguration());
					
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(cp)));
					
					String line;
					while((line=br.readLine()) != null){
						String[] str = line.split("\\s+");
						long[] t=new long[130];
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
		//	log.info("Mapper start");
			
			//log.info(centers.size());
			long maxdist=Long.MAX_VALUE;
			int index=-1;
			
			long[] ints=new long[129];
			LongWritable iw[]=new LongWritable[129];
			
			
			String[] str=value.toString().split("\\s+");
			
			
			for(int i=0;i<128;i++){
				ints[i]=Long.parseLong(str[i+3]);
				iw[i]=new LongWritable(ints[i]);
			}
			
			
			int pos=Integer.parseInt(str[0]);
			
			
			for(int i=pos*k;i<(pos+1)*k;i++){
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
		int k=conf.getInt("k", 10);
		int level=conf.getInt("level",0);
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(HKM.class);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(ClusterReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongArrayWritable.class);
	
		int cc=k;
		for(int t=0;t<level-1;t++){
			cc*=k;
		}	
		String o=OutputPrefix+"/indexd"+String.valueOf(level-1);
	//	for(int i=0;i<cc;i++){
			TextInputFormat.addInputPath(job, new Path(o+"/index-r-00000"));
		//}
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
		int k=conf.getInt("k", 10);
		int ans=0;
		for(int i=0;i<k;i++){
			for(int j=1;j<129;j++){
				ans=ans+Math.abs(oldcenters.get(i)[j]-newcenters.get(i)[j]);
			}
			
		}
		return ans;
	}
	
	
	
	public static int GenNewCenterFile(Configuration conf,String[] args) throws IOException{
		log.info("Generate Center");
		FileSystem fs=FileSystem.get(conf);
		int k=conf.getInt("k", 10);
		int level=conf.getInt("level", 0);
		Path centerPath=new Path(CenterPrefix+String.valueOf(level));
		int cc=k;
		for(int t=0;t<level-1;t++){
			cc*=k;
		}
		
		int tot=0;
		String o=OutputPrefix+"/indexd"+String.valueOf(level-1);
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(centerPath,true)));
		
		
		
	//	for(int i=0;i<cc;i++){
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(o+"/index-r-00000"))));
		String line;
		
		Map[] ha=new Map[cc];
		for(int i=0;i<cc;i++){
			ha[i]=new HashMap<Integer,String>();
		}
		
		
		while((line=br.readLine()) != null){
				
				String[] str = line.split("\\s+");
				int i=Integer.parseInt(str[0]);
				if(ha[i].size()>=k)	continue;
				
				
				String ansStr=String.valueOf(tot)+"\t0\t"+str[3];
				String aStr="";
				
				for(int j=4;j<str.length;j++){
					aStr+=" "+str[j];
				}
				//aStr.hashCode()
				if(!ha[i].containsKey(aStr.hashCode())){
					ha[i].put(aStr.hashCode(), ansStr+aStr+"\n");
					//bw.write(ansStr+aStr+"\n");
					
					tot++;
				}
		}
		
		for(int i=0;i<cc;i++){
			for(Map.Entry<Integer, String> entry:((HashMap<Integer,String>)ha[i]).entrySet()){
				bw.write(entry.getValue());
			}
		}
		br.close();
		//}
		bw.close();
		return tot;
		//String centerDir=OutputPrefix+"/indexd"+String.valueOf(level-1);
	}
	
	
	//arg[0] Input path
	//arg[1] Center file path
	//arg[2] Output path
	//arg[3] Iteration Limit
	//arg[4] K
	//arg[5] MaxDepth
	
	
	public static void main(String[] args) throws Exception{
		log.info("Start HKM");
		
		Configuration conf=new Configuration();
		
		if(args.length>=4)	IterationLimit=Integer.parseInt(args[3]);
		
		int k=Integer.parseInt(args[4]);
		if(args.length>=5){
			conf.setInt("k", k);
		}
		
		if(args.length>=6) MaxDepth=Integer.parseInt(args[5]);
		
		
		FileSystem fs=FileSystem.get(conf);
		
		OutputPrefix=new String(args[2]);
		CenterPrefix=new String(args[1]);

		String[] s=new String[1];
		s[0]=OutputPrefix;
		conf.setStrings("OP", s);
		conf.set("OutputP", OutputPrefix);
	//	conf.setBoolean("ReachLeaf", false);
		
		int success=1;
		int level;
		int sz=k;
		for(level=0;level<MaxDepth;level++){
			conf.setInt("level", level);
		//	if(conf.getBoolean("ReachLeaf", true)) break;
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
					
					//int dist=CheckDistance(conf, args);
				//	log.info("Round "+itr+ " Diff: "+dist);
					//if(itr>1)	fs.delete(new Path(args[1]),true);
					args[1]=args[2]+"/part-r-00000";
					
				}while(itr<IterationLimit);
				if(success==1){
					args[2]=OutputPrefix+"/indexd"+String.valueOf(level);
					ToolRunner.run(conf, new BuildIndex(),args);
					//fs.delete((new Path(args[1])),true);
				}
			}else{
				int gncf=GenNewCenterFile(conf,args);
				sz*=k;
				if(gncf<sz)	break;
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
					
				//	int dist=CheckDistance(conf, args);
				//	log.info("Round "+itr+ " Diff: "+dist);
			//	if(itr>1)	fs.delete(new Path(args[1]),true);
		//			log.info(arg0);
					args[1]=args[2]+"/part-r-00000";
					
				}while(itr<IterationLimit);
				log.info("Call build index");
				if(success==1){
					args[2]=OutputPrefix+"/indexd"+String.valueOf(level);
					args[3]=String.valueOf(level);
					ToolRunner.run(conf, new BuildIndex2(),args);
				//	fs.delete((new Path(args[1])),true);
				}
			}
		}
	}
	

}
