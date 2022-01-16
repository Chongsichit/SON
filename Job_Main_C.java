package homework02_q2c;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job_Main_C extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        /*Job job = Job.getInstance(super.getConf(), "Q2C_1");
        job.setJarByClass(Mapper_1.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155164831/final"));
        FileInputFormat.setMaxInputSplitSize(job,226151483/20);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Mapper_1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reducer_1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155164831/passC_9"));

        job.waitForCompletion(true);*/

        Job job1 = Job.getInstance(super.getConf(), "Q2C_2");
        job1.addCacheFile(new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155164831/passC_9/part-r-00000").toUri());
        job1.setJarByClass(Mapper_2.class);
        FileInputFormat.addInputPath(job1,new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155164831/final"));
        FileInputFormat.setMaxInputSplitSize(job1,226151483/20);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(Mapper_2.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(Reducer_2.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155164831/Result_Q2C_24"));

        return (job1.waitForCompletion(true)? 0:1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf,new Job_Main_C(),args);
        System.exit(run);
    }
}

