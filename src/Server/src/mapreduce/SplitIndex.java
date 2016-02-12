package mapreduce;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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

public class SplitIndex {
	public static void main(String[] args) throws Exception{
		
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0]))));
		String OutPrefix=args[1];
		Path OutPath=new Path(OutPrefix+"/idx"+String.valueOf(0));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(OutPath,true)));
		
		int last=0;
		String line;
		int cnt=0;
		while((line=br.readLine()) != null){
			cnt++;
			String[] str = line.split("\\s+");	
			int num=Integer.parseInt(str[0]);
			if(num!=last){
				System.out.println(num);
				System.out.println(cnt);
				bw.close();
				last=num;
				OutPath=new Path(OutPrefix+"/idx"+String.valueOf(num));
				bw = new BufferedWriter(new OutputStreamWriter(fs.create(OutPath,true)));
			}
			bw.write(line+"\n");
		}
		bw.close();
	}
}
