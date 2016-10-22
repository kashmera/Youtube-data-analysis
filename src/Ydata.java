import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class Ydata {
	public static class Ymap extends MapReduceBase implements Mapper<LongWritable ,Text ,Text, IntWritable>{
      
		@Override
		public void map(LongWritable key, Text Value,OutputCollector<Text, IntWritable> output, Reporter repoter)throws IOException {
        	Text category = new Text();
        	String line = Value.toString();
			String s[]= line.split("\t");	
			if(s.length>5)
			category.set(s[3]);			
			output.collect(category, new IntWritable(1));
			}
			
			
		}
	
	public static class Yred extends MapReduceBase implements Reducer<Text,IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text Key, Iterator<IntWritable> value,OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException {
			int sum=0;
			while(value.hasNext()){
				sum+=value.next().get();
			}
			output.collect(Key, new IntWritable(sum));
		}
		
	}
		
	
	

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(Ydata.class);
		conf.setJobName("YoutubeData Analysis");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
	    conf.setMapperClass(Ymap.class);
	   // conf.setCombinerClass(Yred.class);
	    conf.setReducerClass(Yred.class);
	    conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
        
        
        
		
		
		

	}

}
