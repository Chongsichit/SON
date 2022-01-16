package homework02_q2c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Mapper_1 extends Mapper<LongWritable, Text,Text, IntWritable> {

    int basket_num = 0;
    double threshold = 0.0025;

    List<List<String>> basket = new ArrayList<>();
    HashMap<Set<String>,Integer> hashMap = new HashMap<>();

    List<Set<String>> candidate_list = new ArrayList<>();
    HashMap<Set<String>,Integer> supported_list_2 = new HashMap<>();
    HashMap<Set<String>,Integer> supported_list = new HashMap<>();
    HashMap<Set<String>,Integer> hs = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for(int i = 0; i < split.length ; i++){
            Set<String> item = new HashSet<>(Arrays.asList(split[i]));
            if (hashMap.containsKey(item)){
                hashMap.put(item,hashMap.get(item)+1);
            } else hashMap.put(item,1);
        }
        basket.add(Arrays.asList(split));
        basket_num++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        filter_out(hashMap,hs);
        getSupportedList(hs,supported_list);
        countItemset(supported_list);
        filter_out(supported_list,supported_list_2);

        IntWritable one = new IntWritable(1);

        Set<Set<String>> key_set = supported_list_2.keySet();
        for (Set<String> tmp : key_set){
            List<String> tmp_list = new ArrayList<>(tmp);
            tmp_list.sort(Comparator.naturalOrder());
            context.write(new Text(tmp_list.get(0)+" "+tmp_list.get(1)), one);
        }
    }

    public void filter_out(HashMap<Set<String>,Integer> hashMap,HashMap<Set<String>,Integer> hashMap_2){
        Set<Set<String>> key_set = hashMap.keySet();
        double real_threshold = threshold * basket_num;
        for (Set<String> item_set: key_set){
            int count = hashMap.get(item_set);
            if (count >= real_threshold){
                hashMap_2.put(item_set,count);
                candidate_list.add(item_set);
            }
        }
    }

    public void getSupportedList(HashMap<Set<String>,Integer> hashMap,HashMap<Set<String>,Integer> supported_list){
        for (int i = 0; i < candidate_list.size()-1; i++){
            for (int j = i + 1;j < candidate_list.size();j++){
                Set<String> item1 = candidate_list.get(i);
                Set<String> item2 = candidate_list.get(j);
                HashSet<String> pair = new HashSet<>();
                for(String tmp1: item1){
                    pair.add(tmp1);
                }
                for(String tmp2: item2){
                    pair.add(tmp2);
                }
                supported_list.put(pair,1);
            }
        }
    }

    public void countItemset(HashMap<Set<String>,Integer> supported_list){
        for ( int i = 0; i < basket.size(); i++){
            for ( int j = 0; j < basket.get(i).size()-1; j++){
                for (int x = j + 1; x < basket.get(i).size(); x++){
                    String item_1 = basket.get(i).get(j);
                    String item_2 = basket.get(i).get(x);
                    Set<String> tmp = new HashSet<>();
                    tmp.add(item_1);
                    tmp.add(item_2);
                    if(supported_list.containsKey(tmp)){
                        supported_list.put(tmp,supported_list.get(tmp)+1);
                    } //else supported_list.put(tmp, 1);
                }
            }
        }
    }
}
