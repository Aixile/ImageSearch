package image.search;

import java.io.IOException;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class CustomFixedLengthInputFormat extends FixedLengthInputFormat{
	
	@Override
	public RecordReader<LongWritable,BytesWritable> createRecordReader(InputSplit s,TaskAttemptContext context) throws IOException,InterruptedException{
		int recordLength=136;
		return new FixedLengthRecordReader(recordLength);
		
	}
}
