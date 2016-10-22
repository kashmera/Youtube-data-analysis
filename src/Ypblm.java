import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Ypblm {
	
	public static class Youmap extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable>{

		@Override
		public void map(LongWritable Key, Text Value ,OutputCollector<Text, FloatWritable> Output, Reporter reporter)throws IOException {
			String s[] = Value.toString().split("\t");
			float f = 0;
			if(s.length>7){
				String line=s[6];
				if(line.matches("\\d+.+"))
				  f = Float.parseFloat(s[6]);			
					
			}
			 Output.collect(new Text(s[0]),new FloatWritable(f));		
		}
		
	}
	
	public static class Youreduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>{

		@Override
		public void reduce(Text Key, Iterator<FloatWritable> Value,OutputCollector<Text, FloatWritable> output, Reporter reporter)throws IOException {
			float sum =0; int count=0;
			while(Value.hasNext()){
				count=count+1;
				sum+=Value.next().get();
				
			}
			sum=sum/count;
         output.collect(Key,new FloatWritable(sum));
		}
		
		
		
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(Ypblm.class);
		conf.setJobName("Top 10 rated videos");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setMapperClass(Youmap.class);
		conf.setReducerClass(Youreduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		

	}

}
