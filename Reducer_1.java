package homework02_q2c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer_1 extends Reducer<Text, IntWritable,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}