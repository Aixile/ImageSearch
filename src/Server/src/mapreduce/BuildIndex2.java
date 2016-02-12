package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BuildIndex2  extends Configured implements Tool  {
	private static Log log=LogFactory.getLog(BuildIndex2.class);
	//private static int level=10;
	
	
	public static class ClusterMapper extends Mapper<LongWritable,Text,IntWritable,IntArrayWritable>{
		//private final static IntWritable one=new IntWritable(1);
		
	//	Vector<byte[]> centers=new Vector<byte[]>(); 
		
		Vector<int[]> centers=new Vector<int[]>();
		int k;
		@Override
		public void setup(Context context) throws IOException{

					log.info("Mapper setup start");
					URI[] cache=context.getCacheFiles();
					k=context.getConfiguration().getInt("k",10);
					int level=context.getConfiguration().getInt("level", 0);
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
	//		log.info("Mapper start");
			byte[] b=value.getBytes();
			
			//log.info(centers.size());
			int maxdist=Integer.MAX_VALUE;
			int index=-1;
			
			int[] ints=new int[136];
			
		
			IntWritable iw[]=new IntWritable[136];
			
			String[] str=value.toString().split("\\s+");
			
			
			for(int i=0;i<128;i++){
				ints[i]=Integer.parseInt(str[i+3]);
				iw[i]=new IntWritable(ints[i]);
			}
			
			for(int i=0;i<8;i++){
				//log.info(b[i]);
				
				int hv=0,lv=0;
				char hc=str[2].charAt(i*2),lc=str[2].charAt(i*2+1);
				if(hc>='a'&&hc<='f') 	hv=10+hc-'a';
				else hv=hc-'0';
				if(lc>='a'&&lc<='f') 	lv=10+lc-'a';
				else lv=lc-'0';
				
				ints[135-i]=hv*16+lv;
				
			//	log.info(ints[i+128]);
				iw[135-i]=new IntWritable(ints[135-i]);
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
	
	
	public static class BuildIndexReducer extends Reducer<IntWritable,IntArrayWritable,IntWritable,Text>{
		private MultipleOutputs<IntWritable,Text> mos;	
		int k;
		@Override
		public void setup(Context context) {
			mos=new MultipleOutputs<IntWritable,Text>(context);
			k=context.getConfiguration().getInt("k", 10);
		} 
		 
		@Override
		public void reduce(IntWritable key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			log.info("Start BuildIndexReducer on "+String.valueOf(key.get()));
			int cnt=0;
			while(values.iterator().hasNext()){
				Writable[] t=values.iterator().next().get();
				String str="";
				
				for(int i=0;i<128;i++){
					str=str+" "+String.valueOf(((IntWritable) t[i]).get());
				}
				String str2="";
				for(int i=135;i>=128;i--){
				
					int hi=((IntWritable) t[i]).get()/16,lo=((IntWritable) t[i]).get()%16;
					char h=(char) (hi>=10?'a'+(hi-10):'0'+hi);
					char l=(char) (lo>=10?'a'+(lo-10):'0'+lo);
					str2=str2+h+l;
					//str2=str2+Integer.toHexString(((IntWritable) t[i]).get());
				}
				//cnt+=((IntWritable) t[128]).get();
				cnt++;
				mos.write("index", key, new Text(String.valueOf(cnt)+" "+str2+" "+str));
			}
			//if(cnt<k){
			//	context.getConfiguration().setBoolean("ReachLeaf", true);
		//		xxHKM.ReachLeaf=true;
			//}
			context.write(key, new Text(String.valueOf(cnt)));
		}	
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	

	@Override
	public int run(String[] args) throws Exception{
		
	for(int i=0;i<args.length;i++) System.out.println(args[i]);
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		Job job=Job.getInstance(conf,"BuildIndex2 "+args[2]);
		
		int level=Integer.parseInt(args[3]);
		conf.setInt("level", level);
		int k=conf.getInt("k",10);	
		
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(BuildIndex2.class);
		job.setMapperClass(ClusterMapper.class);
		job.setReducerClass(BuildIndexReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
	
		
		
		int cc=k;
		for(int t=0;t<level-1;t++){
			cc*=k;
		}	
		String OutputPrefixA[]=conf.getStrings("OP");
		String OutputPrefix=OutputPrefixA[0];
		String o=OutputPrefix+"/indexd"+String.valueOf(level-1);
		
		//for(int i=0;i<cc;i++){
			TextInputFormat.addInputPath(job, new Path(o+"/index-r-00000"));
	//	}
		job.setInputFormatClass(TextInputFormat.class);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
	/*	
		CustomFixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFixedLengthInputFormat.class);*/
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		//for(int i=0;i<cc*k;i++){
			MultipleOutputs.addNamedOutput(job,"index", TextOutputFormat.class, IntWritable.class, Text.class);
	//	}
		
		return job.waitForCompletion(true)?0:1;
	}
/*
	public static void main(String[] args) throws Exception{
		log.info("Start HKM");
		
		Configuration conf=new Configuration();
		
		int k=Integer.parseInt(args[4]);
		if(args.length>=5){
			conf.setInt("k", k);
		}
		
		FileSystem fs=FileSystem.get(conf);
		
		String OutputPrefix=new String(args[2]);
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
	*/

	
}
