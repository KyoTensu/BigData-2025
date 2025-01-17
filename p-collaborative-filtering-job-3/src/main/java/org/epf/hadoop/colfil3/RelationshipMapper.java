package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        String newKeyUser1 = parts[0].substring(0, parts[0].indexOf("."));
        String newValueUser1 = parts[0].substring(parts[0].indexOf(".")+1)+","+parts[1];
        String newKeyUser2 = parts[0].substring(parts[0].indexOf(".")+1);
        String newValueUser2 = parts[0].substring(0, parts[0].indexOf("."))+","+parts[1];

        context.write(new Text(newKeyUser1), new Text(newValueUser1));
        context.write(new Text(newKeyUser2), new Text(newValueUser2));
    }
}
