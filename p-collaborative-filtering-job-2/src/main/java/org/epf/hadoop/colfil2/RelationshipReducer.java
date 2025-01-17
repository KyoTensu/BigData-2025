package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    public void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;

        for(IntWritable val : values) {
            if(val.get() == 1) {
                counter++;
            }else{
                counter = 0;
                break;
            }
        }

        if(counter != 0) {
            context.write(key, new IntWritable(counter));
        }
    }
}
