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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class BuildIndex  extends Configured implements Tool  {
	private static int k=10;
	private static Log log=LogFactory.getLog(BuildIndex.class);
	
	
	
	public static class ClusterMapper extends Mapper<LongWritable,BytesWritable,IntWritable,IntArrayWritable>{
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
			
			int[] ints=new int[136];
			
		
			IntWritable iw[]=new IntWritable[136];
			
			for(int i=0;i<128;i++){
				ints[i]=b[i+8]&0xFF;
				iw[i]=new IntWritable(ints[i]);
			}
			
			for(int i=0;i<8;i++){
				//log.info(b[i]);
				ints[i+128]=b[i]&0xFF;
			//	log.info(ints[i+128]);
				iw[i+128]=new IntWritable(ints[i+128]);
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
	
	
	public static class BuildIndexReducer extends Reducer<IntWritable,IntArrayWritable,IntWritable,Text>{
		private MultipleOutputs<IntWritable,Text> mos;	
		@Override
		public void setup(Context context) {
			mos=new MultipleOutputs<IntWritable,Text>(context);
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
				mos.write("index"+key.toString(), key, new Text(String.valueOf(cnt)+" "+str2+" "+str));
			}
			if(cnt<k)	HKM.ReachLeaf=true;
			context.write(key, new Text(String.valueOf(cnt)));
		}	
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	

	@Override
	public int run(String[] args) throws Exception{
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		Job job=Job.getInstance(conf,"BuildIndex "+args[2]);
		if(args.length>=5) 	k=Integer.parseInt(args[4]);
		
		
		job.addCacheFile((new Path(args[1])).toUri());
		
		job.setJarByClass(BuildIndex.class);
		job.setMapperClass(ClusterMapper.class);
		job.setReducerClass(BuildIndexReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		CustomFixedLengthInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFixedLengthInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		for(int i=0;i<k;i++){
			MultipleOutputs.addNamedOutput(job,"index"+String.valueOf(i), TextOutputFormat.class, IntWritable.class, Text.class);
		}
		
		return job.waitForCompletion(true)?0:1;
		
	}
}
