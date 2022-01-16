package homework02_q2c;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Mapper_2 extends  Mapper<LongWritable, Text, Text, IntWritable> {


    HashMap<Set<String>,Integer> cacheFile = new HashMap<>();
    int basket_num = 0;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Path[] label = context.getLocalCacheFiles();
        FileReader fileReader = new FileReader(String.valueOf(label[0]));
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String item;
        while((item = bufferedReader.readLine())!= null){
            String[] split = item.split(" ");
            HashSet<String> temp = new HashSet<>();
            for (int i=0; i<split.length; i++){
                temp.add(split[i]);
            }
            cacheFile.put(temp,0);
        }

        bufferedReader.close();
        fileReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        List<String> temp = new ArrayList<>(Arrays.asList(split));
        temp.sort(Comparator.naturalOrder());
        if (temp.size() >= 3){
            for (int x = 0; x < temp.size()-2; x++){
                for (int y = x + 1; y<temp.size()-1; y++){
                    for (int z = y + 1; z <temp.size(); z++){
                        Set<String> hs1 = new HashSet<>(Arrays.asList(temp.get(x),temp.get(y)));
                        Set<String> hs2 = new HashSet<>(Arrays.asList(temp.get(x),temp.get(z)));
                        Set<String> hs3 = new HashSet<>(Arrays.asList(temp.get(y),temp.get(z)));
                        if (cacheFile.containsKey(hs1)&&cacheFile.containsKey(hs2)&&cacheFile.containsKey(hs3)){
                            context.write(new Text(split[x]+" "+split[y]+" "+ split[z]), new IntWritable(1));
                        }
                    }
                }
            }
        }
        basket_num++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.write(new Text("basket: "), new IntWritable(basket_num));
    }
}
