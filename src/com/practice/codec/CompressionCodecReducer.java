package com.practice.codec;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompressionCodecReducer  extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	public void reduce(Text key,Iterable<IntWritable> itr,Context context) throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable itr2:itr){
			sum = sum+ itr2.get();
			
		}
		context.write(key, new IntWritable(sum));
	}

}
