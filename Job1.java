import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Job1 {
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text month = new Text();
		private Text adjClose = new Text(); 
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			if(key.get()>=1){
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			String line = value.toString(); //receive one line
			//System.out.println(line);
			String[] element = line.split(",");
			if(!element[0].equals("Date")){
			String[] date = element[0].split("-");
//			System.out.println(date[0]+ " " + date[1] + " " + date[2]);
			String keyOrg = date[0].concat(date[1]);
			 
//			System.out.println(keyOrg);
				month.set(fileName+"/"+ keyOrg); //take the column of A as key.
				String unprocessed = element[6]+"/"+date[2];
			//	System.out.println(unprocessed);
				adjClose.set(unprocessed);
			//	System.out.println("<"+month.toString()+"\t"+adjClose.toString()+">");
				context.write(month, adjClose);
			}//end of if
		}
	}		
}


	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		private Text year = new Text();
		private Text xi = new Text();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			
			double endvalue = 0, startvalue = 0;
			Map<Integer, Double> treemap = new TreeMap<Integer, Double>();
			for(Text x: values){
				String[] y = x.toString().split("/");
				//System.out.println(y[0]+" "+y[1]);
				treemap.put(Integer.parseInt(y[1]), Double.parseDouble(y[0]));
//				System.out.println(key);
			}
			
			
			List<Double> list = new ArrayList<Double>(treemap.values());
			//System.out.println("-------------------------------------------------Ending values----------------------------------------------------------------------");
			startvalue = Double.parseDouble(list.get(0).toString());
			endvalue = Double.parseDouble(list.get(list.size()-1).toString());
			//System.out.println(start);
			double diff = (endvalue - startvalue)/startvalue;
			String k = Double.toString(diff);
			//System.out.println(key +" "+ end +" - "+start+ " "+ k);
					year.set(key);
					xi.set(k);
					//System.out.println(year + " "+ xi);
					context.write(year, xi); 	
				}
			}
	}
		
