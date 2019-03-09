package com.practice.codec;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CompressionCodecMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		StringTokenizer strToken = new StringTokenizer(value.toString());
		while(strToken.hasMoreTokens()){
			context.write(new Text(strToken.nextToken()), new IntWritable(1));
		}
		
		
	}

}
