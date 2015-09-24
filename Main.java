
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {
	

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();	
		System.out.println("************************TIMER STARTED********************************");
		//Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(Main.class);
		Job job3 = Job.getInstance();
		job3.setJarByClass(Main.class);
		Job job2 = Job.getInstance();
		job2.setJarByClass(Main.class);

		System.out.println("\n----------------Job1----------------\n");
		job.setJarByClass(Main.class);
		job.setMapperClass(Job1.Map1.class);
		job.setReducerClass(Job1.Reduce1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

	//	int NOfReducer1 = Integer.valueOf(args[1]);	
	//	job.setNumReduceTasks(NOfReducer1);




		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path("Inter_"+args[1]));


		job.waitForCompletion(true);

		System.out.println("\n----------------Job2----------------\n");

		job2.setJarByClass(Main.class);

		job2.setMapperClass(Job2.Map2.class);
		job2.setReducerClass(Job2.Reduce2.class);
		//int NOfReducer2 = Integer.valueOf(args[1]);	
		//job2.setNumReduceTasks(NOfReducer2);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
		FileOutputFormat.setOutputPath(job2,new Path("Inter2_"+args[1]));


		job2.waitForCompletion(true);

		System.out.println("\n----------------Job3----------------\n");


		job3.setJarByClass(Main.class);
		job3.setMapperClass(Job3.Map3.class);
		job3.setReducerClass(Job3.Reduce3.class);
	//	int NOfReducer3 = Integer.valueOf(args[1]);	
	//	job3.setNumReduceTasks(NOfReducer3);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job3, new Path("Inter2_"+args[1]));
		FileOutputFormat.setOutputPath(job3,new Path("Output_"+args[1]));




		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("************************TIMER ENDED********************************");
			System.out.println("\nJob took " + (end-start)/1000 + " seconds\n");
		}
	}
}

