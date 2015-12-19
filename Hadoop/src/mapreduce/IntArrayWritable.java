package mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {
	public IntArrayWritable(){
		super(IntWritable.class);
	}
	public String  toString(){
		Writable[] w=get();
		String ans="";
		for(int i=0;i<w.length;i++){
			int v=(((IntWritable)w[i])).get();
			if(i!=0)	ans=ans+" "+String.valueOf(v);
			else ans=String.valueOf(v);
		}
		return ans;
	}
}
