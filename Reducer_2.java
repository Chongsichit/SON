package homework02_q2c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class Reducer_2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*private Integer baskets = 0;
    private Map<Set<String>, Double> map = new HashMap<>();
    private Double min_support = 0.0025;
    private Integer limited = 20;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        String[] split = key.toString().split(" ");
        Double counter = 0.0;
        String str = key.toString();
        System.out.println("map size");
        System.out.println(map.size());
        if (!str.equals("baskets")) {
            for (IntWritable value : values) {
                counter = counter + 1.0;
            }
            Set<String> set = new HashSet<>(Arrays.asList(split));
            if (!map.containsKey(set)) {
                map.put(set, counter);
            }else {
                map.put(set, map.get(set) + counter);
            }
        }else {
            for (IntWritable value : values) {
                baskets += Integer.parseInt(value.toString());
            }
        }

    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Map<Set<String>, Double> mapAno = new HashMap<>();

        for (Map.Entry<Set<String>, Double> entry : map.entrySet()) {
            if (entry.getValue() >= min_support * baskets) {
                mapAno.put(entry.getKey(), entry.getValue());
            }
        }
        System.out.println("mapAno size");
        System.out.println(mapAno.size());
        List<Map.Entry<Set<String>, Double>> list = new ArrayList<>(mapAno.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Set<String>, Double>>() {
            @Override
            public int compare(Map.Entry<Set<String>, Double> o1, Map.Entry<Set<String>, Double> o2) {
                int compare = o1.getValue().compareTo(o2.getValue());
                return -compare;
            }
        });

        int i = 0;
        for (Map.Entry<Set<String>, Double> entry : list) {
            List<String> keyList = new ArrayList<>(entry.getKey());
            context.write(new Text(keyList.get(0) + " " + keyList.get(1) + " " + keyList.get(2) + " " + entry.getValue()), NullWritable.get());
            i++;
            if (i == limited) {
                break;
            }
        }
}*/



    int basket_num = 0;
    double threshold = 0.0025;
    HashMap<Set<String>,Integer> hashMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        if(key.toString().equals("basket: ")){
            for(IntWritable value: values){
                basket_num+= value.get();
            }
        } else {
            String[] split = key.toString().split(" ");
            HashSet<String> hashSet = new HashSet<>(Arrays.asList(split));
            int count = 0;
            for (IntWritable value:values){
                count+=value.get();
            }
            if (hashMap.containsKey(hashSet)){
                int total = hashMap.get(hashSet) + count;
                hashMap.put(hashSet,total);
            } else hashMap.put(hashSet,count);
        }
    }


    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Set<Set<String>> key_1 = hashMap.keySet();
        double real_threshold = basket_num*threshold;
        HashMap<Set<String>, Integer> hs = new HashMap<>();

        for (Set<String> key1: key_1){
            int count = hashMap.get(key1);
            if ( count >= real_threshold){
                hs.put(key1,count);
            }
        }

        hashMap.clear();

        List<HashMap.Entry<Set<String>, Integer>> list = new ArrayList<>(hs.entrySet());
        Collections.sort(list, new Comparator<HashMap.Entry<Set<String>, Integer>>() {
            @Override
            public int compare(Map.Entry<Set<String>, Integer> o1, Map.Entry<Set<String>, Integer> o2) {
                int compare = o1.getValue().compareTo(o2.getValue());
                return -compare;
            }
        });

        int i = 0;

        for(Map.Entry<Set<String>, Integer> key2: list ){
            if ( i <= 20){
                List<String> temp = new ArrayList<>(key2.getKey());
                context.write(new Text("{"+temp.get(0)+" "+temp.get(1)+" "+temp.get(2)+"}"), new IntWritable(hs.get(key2)));
                i++;
            }
        }
    }
}
