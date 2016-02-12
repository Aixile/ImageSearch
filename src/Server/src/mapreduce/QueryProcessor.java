package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import mapreduce.HKM.ClusterMapper;
import mapreduce.HKM.ClusterReducer;
import mapreduce.HKM.Combiner;
import netthrow.NetThrow;

public class QueryProcessor extends Configured implements Tool {
	
	public static Vector<Vector<int[]>>  centers;
//	public static HashMap<Integer, Vector<int[]>>  dataMap;
	
/*	
	public static int IterationLimit=6;
	public static String InputPrefix="";
	public static String CenterPrefix="";
	public static String OutputPrefix="";

	public static int MaxDepth=2;
	public static int k=10;
	public static int VoteLimit=10;
	*/
	
	public static String HDFSTmpPath="";
	
	private static Log log=LogFactory.getLog(QueryProcessor.class);	
	
	private static Vector<Vector<int[]>>  init(FileSystem fs,Configuration conf) throws IllegalArgumentException, IOException{
		int MaxDepth=conf.getInt("MaxDepth",3);
		int IterationLimit=conf.getInt("IterationLimit",20);
		String[] args=conf.getStrings("ARGS");
		String InputPrefix=new String(args[0]);
			//for(int i=0;i<MaxDepth;i++)	centers.add(new Vector<int[]>());
		Vector<Vector<int[]>>  centers=new Vector<Vector<int[]>>() ;
		for(int i=0;i<MaxDepth;i++){
			Vector<int[]> ans=new Vector<int[]>();
			
			String strP=InputPrefix+"/d"+String.valueOf(i)+"r"+String.valueOf(IterationLimit);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(strP+"/part-r-00000"))));
			String line;
			while((line=br.readLine()) != null){
				String[] str = line.split("\\s+");	
				int[] arr=new int[129];
				for(int j=2;j<str.length;j++){
					arr[j-2]=Integer.parseInt(str[j]);
				}
				arr[128]=Integer.parseInt(str[1]);
				ans.add(arr);
			}
			centers.add(ans);
			log.info("Depth:"+String.valueOf(i)+" Size: "+String.valueOf(ans.size()));
		}
		return centers;
	}
	
	public static class IndexFileMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		public  HashMap<Integer, Vector<int[]>>  dataMap;
		
		@Override
		public void setup(Context context) throws IOException{
		
			URI[] cache=context.getCacheFiles();
			FileSystem fs=FileSystem.get(context.getConfiguration());
			if(cache == null || cache.length <=0) System.exit(1);
		//	String[] idxAll2=context.getConfiguration().getStrings("IndexAll");
			String idxAll=context.getConfiguration().get("idx");
			String[] vidx=idxAll.split(";");
			
			dataMap=new HashMap<Integer, Vector<int[]>>(); 
			
			Path cp=new Path(cache[0]);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(cp)));
			String line;
			int cnt=0;
			
			
			
			while((line=br.readLine()) != null){
				String[] str = line.split("\\s+");
				int[] t=new int[130];
				for(int i=2;i<str.length;i++){
					t[i-2]=Integer.parseInt(str[i]);
				}
				
				t[128]=Integer.parseInt(str[0]);
				String[] idx=vidx[cnt].split("\\s+");
				for(String i:idx){
					int idxi=Integer.parseInt(i);
					if(!dataMap.containsKey(idxi)){
						dataMap.put(idxi, new Vector<int[]>());
						dataMap.get(idxi).add(t);
					}else{
						dataMap.get(idxi).add(t);
					}	
				}
				cnt++;
			}
			br.close();
		}
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			log.info("Mapper start");
			
			String[] str=value.toString().split("\\s+");
			int idx=Integer.parseInt(str[0]);
			Vector<int[]> data=dataMap.get(idx);
			
			
			int[] v=new int[128];
			for(int i=3;i<str.length;i++){
				v[i-3]=Integer.parseInt(str[i]);
			}
			
			
			for(int i=0;i<data.size();i++){
				int dist=0;
				for(int j=0;j<128;j++){
					dist+=Math.abs(v[j]-data.get(i)[j]);
				}
				String astr=String.valueOf(dist)+" "+str[2];
				
				context.write(new IntWritable(data.get(i)[128]), new Text(astr));
			}
		}
		
	}
	
	public static class VoteReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		int VoteLimit;
		@Override
		public void setup(Context context) throws IOException{

					log.info("VoteReducer setup start");
					VoteLimit=context.getConfiguration().getInt("VoteLimit",10);
					
		}
		
		@Override
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			log.info("VoteReducer start");
			TreeMap<Integer,Vector<String>> tm=new TreeMap<Integer,Vector<String>>();
			HashSet<String> hs=new HashSet<String>();
			
			while(values.iterator().hasNext()){
				String[]  str=values.iterator().next().toString().split("\\s+");
				int w=Integer.parseInt(str[0]);
				if(tm.containsKey(w)){
					tm.get(w).add(str[1]);
				}else{
					tm.put(w, new Vector<String>());
					tm.get(w).add(str[1]);
				}
			}
			int cnt=0;
			for(Map.Entry<Integer, Vector<String>> entry :tm.entrySet()){
				 Vector<String> v=entry.getValue();
				 for(int i=0;i<v.size();i++){
					 cnt++;
					 if(!hs.contains(v.get(i))){
						 context.write(key, new Text(v.get(i)));
						 hs.add(v.get(i));
					 }
				 }
				 if(cnt>VoteLimit)	break;
			}
		}
	}
	
	
	public static Vector<Integer> GetIndexID(int[] v,Configuration conf){
		//int at=0;
		//log.info(centers.size());
		int MaxDepth=conf.getInt("MaxDepth",3);
		int k=conf.getInt("k",3);
		
		Vector<Integer> buf=new Vector<Integer>();
		buf.add(0);
		
		for(int i=0;i<MaxDepth;i++){
			
			TreeMap<Integer, TreeSet<Integer>> tm=new TreeMap<Integer, TreeSet<Integer>>();
			for(int t=0;t<buf.size();t++){
				buf.set(t, buf.get(t)*k);
				for(int j=buf.get(t)+0;j<buf.get(t)+k;j++){
					int ans=0;
					for(int d=0;d<128;d++){
						int q=centers.get(i).get(j)[d];
						ans+=Math.abs(q-v[d]);
					}
					if(!tm.containsKey(ans)){
						tm.put(ans, new TreeSet<Integer>() );
						tm.get(ans).add(j);
					}else{
						tm.get(ans).add(j);
					}
				}
			}
			buf.clear();
			int cnt=0;
			for(Map.Entry<Integer,TreeSet<Integer>> entry:tm.entrySet()){
				if(cnt>=1) break;
				TreeSet<Integer> ts=entry.getValue();
				for(Integer s:ts){
					buf.add(s);
					cnt++;
					if(cnt>=1) break;
				}
			}
		}
		return buf;
	}
	
	public static void ReadQueryData(Path p,FileSystem fs,Job job, String opath,Configuration conf) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
		HashMap<Integer, Integer>  dataMap=new HashMap<Integer,Integer> ();
		
		String line;
		String idxAll="";
		
		while((line=br.readLine()) != null){
			String[] str = line.split("\\s+");
			int[] t=new int[130];
			for(int i=2;i<str.length;i++){
				t[i-2]=Integer.parseInt(str[i]);
			}
			
			t[128]=Integer.parseInt(str[0]);
			
			//int idx=GetIndexID(t,conf);
			Vector<Integer> vidx=GetIndexID(t,conf);
			for(Integer idx:vidx){
				idxAll=idxAll+String.valueOf(idx)+" ";
				if(!dataMap.containsKey(idx)){
					dataMap.put(idx, 1);
			//		dataMap.get(idx).add(t);

					TextInputFormat.addInputPath(job, new Path(opath+"/idx"+String.valueOf(idx)));
				}	
			}
			idxAll=idxAll+";";
			
			/*else{
				dataMap.get(idx).add(t);
			}*/
		}
		
		//String[] idxAll2=new String[2];
	//	idxAll2[0]=idxAll;
		job.getConfiguration().set("idx",idxAll);
		System.out.println(idxAll);
		
		//System.out.println(conf.get("idx"));
	}
	
	//args[0] Query File Path
	//args[1] Output File Dir
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);	
		if(centers==null){
			centers=init(fs,conf);
		}
		
		int MaxDepth=conf.getInt("MaxDepth",3);
	//	int IterationLimit=conf.getInt("IterationLimit",20);
		String[] argsProgram=conf.getStrings("ARGS");
		String InputPrefix=new String(argsProgram[0]);
	
		Job job=Job.getInstance(conf,"Query Processor: ");
		
		
	//	dataMap=new HashMap<Integer, Vector<int[]>> ();
		
		job.setJarByClass(QueryProcessor.class);
		job.setMapperClass(IndexFileMapper.class);
		job.setReducerClass(VoteReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.addCacheFile((new Path(args[0])).toUri());
		
		ReadQueryData(new Path(args[0]), fs, job, InputPrefix+"/indexd"+String.valueOf(MaxDepth-1),conf);
		job.setInputFormatClass(TextInputFormat.class);
			
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		return job.waitForCompletion(true)?0:1;
	}
	
	
	//hadoop jar KMeans.jar mapreduce.QueryProcessor hdfs:///out1 hdfs:///center4 hdfs:///out6 4 3 10 /tmp 5

	//args[0] Indexing output prefix
	//args[1] Center Prefix
	//args[2] Output prefix
	//args[3] k
	//args[4] MaxDepth
	//args[5] Iterlimit
	//args[6] HDFS Tmp path
	//args[7] Vote Limit
	
	public static void main(String[] args) throws Exception{
		/*InputPrefix=new String(args[0]);
		CenterPrefix=new String(args[1]);
		OutputPrefix=new String(args[2]);
		k=Integer.parseInt(args[3]);
		
		IterationLimit=Integer.parseInt(args[5]);

		VoteLimit=Integer.parseInt(args[7]);
		*/
		HDFSTmpPath=new String(args[6]);
		
		Configuration conf=new Configuration();
		conf.setStrings("ARGS", args);
		conf.setInt("k", Integer.parseInt(args[3]));
		conf.setInt("MaxDepth", Integer.parseInt(args[4]));
		conf.setInt("IterationLimit", Integer.parseInt(args[5]));
		conf.setInt("VoteLimit", Integer.parseInt(args[7]));
		
		//init(fs);
		
		
        NetThrow netThrow = new NetThrow(conf);
        netThrow.Start();
	//	centers=new (Vector<int[]>) [k] ;
	}

}
