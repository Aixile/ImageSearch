package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

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
	public static HashMap<Integer, Vector<int[]>>  dataMap;
	
	
	public static int IterationLimit=6;
	public static String InputPrefix="";
	public static String CenterPrefix="";
	public static String OutputPrefix="";
	public static String HDFSTmpPath="";
	public static int MaxDepth=2;
	public static int k=10;
	public static int VoteLimit=10;
	
	private static Log log=LogFactory.getLog(QueryProcessor.class);	
	private static void init(FileSystem fs) throws IllegalArgumentException, IOException{
		
			//for(int i=0;i<MaxDepth;i++)	centers.add(new Vector<int[]>());
		centers=new Vector<Vector<int[]>>() ;
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
	}
	
	public static class IndexFileMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
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
	
	
	public static int GetIndexID(int[] v){
		int at=0;
		log.info(centers.size());
		
		for(int i=0;i<MaxDepth;i++){
			at*=k;
			
			log.info(centers.get(i).size());
			int mindist=Integer.MAX_VALUE;
			int index=-1;
			for(int j=at+0;j<at+k;j++){
				
				int ans=0;
				for(int d=0;d<128;d++){
					int q=centers.get(i).get(j)[d];
					ans+=Math.abs(q-v[d]);
				}
				if(ans<mindist){
					index=j;
					mindist=ans;
				}	
				
				
			}
			at=index;
			log.info("At "+String.valueOf(at));
		}
		return at;
	}
	
	public static void ReadQueryData(Path p,FileSystem fs,Job job, String opath) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
		
		String line;
		while((line=br.readLine()) != null){
			String[] str = line.split("\\s+");
			int[] t=new int[130];
			for(int i=2;i<str.length;i++){
				t[i-2]=Integer.parseInt(str[i]);
			}
			
			t[128]=Integer.parseInt(str[0]);
			
			int idx=GetIndexID(t);
			log.info(idx);
			if(!dataMap.containsKey(idx)){
				dataMap.put(idx, new Vector<int[]>());
				dataMap.get(idx).add(t);

				TextInputFormat.addInputPath(job, new Path(opath+"/index"+String.valueOf(idx)+"-r-00000"));
			}else{
				dataMap.get(idx).add(t);
			}
		}
		
	}
	
	//args[0] Query File Path
	//args[1] Output File Dir
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);	
		
		//init(fs);
		dataMap=new HashMap<Integer, Vector<int[]>> ();
		Job job=Job.getInstance(conf,"Query Processor: ");
		
		job.setJarByClass(QueryProcessor.class);
		job.setMapperClass(IndexFileMapper.class);
		job.setReducerClass(VoteReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		ReadQueryData(new Path(args[0]), fs, job, InputPrefix+"/indexd"+String.valueOf(MaxDepth-1));
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
		InputPrefix=new String(args[0]);
		CenterPrefix=new String(args[1]);
		OutputPrefix=new String(args[2]);
		k=Integer.parseInt(args[3]);
		MaxDepth=Integer.parseInt(args[4]);
		IterationLimit=Integer.parseInt(args[5]);
		HDFSTmpPath=new String(args[6]);
		VoteLimit=Integer.parseInt(args[7]);
		
		
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		
		init(fs);
		
		
        NetThrow netThrow = new NetThrow(conf);
        netThrow.Start();
	//	centers=new (Vector<int[]>) [k] ;
	}

}
