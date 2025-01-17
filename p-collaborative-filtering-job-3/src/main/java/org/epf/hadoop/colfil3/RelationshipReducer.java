package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> valuesList = new ArrayList<>();
        List<String> namesList = new ArrayList<>();
        for (Text value : values) {
            valuesList.add(value.toString());
        }

        valuesList.sort((a, b) -> {
            String valuea = a.substring(a.indexOf(",")+1);
            String valueb = b.substring(b.indexOf(",")+1);
            return Integer.parseInt(valuea) - Integer.parseInt(valueb);
        });

        for (int i = 0; i < valuesList.size() && i < 5; i++) {
            if(!namesList.contains(valuesList.get(i).substring(0, valuesList.get(i).indexOf(",")))) {
                namesList.add(valuesList.get(i).substring(0, valuesList.get(i).indexOf(",")));
            }
        }

        context.write(key, new Text(String.join(",", namesList)));
    }
}
