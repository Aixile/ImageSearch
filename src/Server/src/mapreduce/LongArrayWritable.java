package mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable 	extends ArrayWritable {
		public LongArrayWritable(){
			super(LongWritable.class);
		}
		public String  toString(){
			Writable[] w=get();
			String ans="";
			for(int i=0;i<w.length;i++){
				long v=(((LongWritable)w[i])).get();
				if(i!=0)	ans=ans+" "+String.valueOf(v);
				else ans=String.valueOf(v);
			}
			return ans;
		}
	}

