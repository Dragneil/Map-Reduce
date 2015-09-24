import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3 {
	
	public static class Map3 extends Mapper<Object, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); 
			String element[] = null;
			element = line.split("	");
			Integer one = 1;
			key1.set(one.toString());
			value1.set(element[0]+"/"+element[1]);
		//	System.out.println(key1+" "+value1);
			context.write(key1, value1);
		}
	}

	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
	
		private Text value = new Text();
		double sum = 0;
		double y = 0;
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			
			Map<String, Double> map = new HashMap<String, Double>();
			int N=0;
			for (Text value:values){
				String[] h = value.toString().split("/");
			//	System.out.println(h[0]+ "    "+h[1]);
				map.put(h[1].toString(), Double.parseDouble(h[0]));
				N +=1;
			}
			Map<String, Double> sortedmap = SortByValue(map);
//			Map<Double, String> reversemap = new HashMap<Double,String>(Collections.reverseOrder());
			List<String> list1 = new ArrayList<String>(sortedmap.keySet());
		//	for(String x:list1){
		//		System.out.println(x);
		//	}
			List<Double> keylist1 = new ArrayList<Double>(sortedmap.values());
			List<String> sublist1 = new ArrayList<String>(list1.subList(0, 11));
			List<Double> keysublist1 = new ArrayList<Double>(keylist1.subList(0, 11));
			//List<String> list2 = new ArrayList<String>(reversetreemap.values());
			List<String> sublist2 = new ArrayList<String>(list1.subList(sortedmap.size()-10, sortedmap.size()));
			
			//List<Double> keylist2 = new ArrayList<Double>(sortedmap.values());
			List<Double> keysublist2 = new ArrayList<Double>(keylist1.subList(sortedmap.size()-11, sortedmap.size()));
	
			
			String finalList = "--------Highest volatility------------" + "\n";
			//System.out.println("-------Highest volatility------------");
			for(int i = 0; i<sublist1.size()-1;i++){
				
				finalList += sublist1.get(i) + "\n";
			//	System.out.println(sublist1.get(i)+ "   "+ keysublist1.get(i));
			}
			//System.out.println("-------Lowest volatility------------");
			finalList = finalList + "--------Lowest volatility------------" + "\n";
			for(int i = 0; i<sublist2.size();i++){
				//System.out.println(list1.indexOf(sublist2.get(i)));
				finalList += sublist2.get(i) + "\n";
			//	System.out.println(sublist2.get(i)+ "   "+ keysublist2.get(i));
			}
			
		
			key.set(" ");
			value.set(finalList);
			context.write(key,value);
		}
		//Reference: http://stackoverflow.com/questions/12738216/sort-hashmap-with-duplicate-values
		static Map<String, Double> SortByValue(Map<String, Double> unsortedmap) {
			// TODO Auto-generated method stub
			List list = new LinkedList(unsortedmap.entrySet());
			//System.out.println(list.size()+ "Size");
			Collections.sort(list,new Comparator(){
				@Override
				public int compare(Object o1, Object o2) {
	                return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
	            }
			});
			//int M = 0;
			Map result = new LinkedHashMap();
			Iterator it = list.iterator();
			while (it.hasNext()) {
	            Map.Entry entry = (Map.Entry) it.next();
	            result.put(entry.getKey(), entry.getValue());
	        //    M +=1;
	        }
			//System.out.println(M);
			return result;
		}
	}
	
}
