package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    public void map(LongWritable key, Relationship value, Context context
    ) throws IOException, InterruptedException {
        Relationship relation = value;
        context.write(new Text(relation.getId1()), new Text(relation.getId2()));
        context.write(new Text(relation.getId2()), new Text(relation.getId1()));
    }
}