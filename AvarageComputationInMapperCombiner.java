import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvarageComputationInMapperCombiner extends Configured implements Tool
{
	public static class ApacheAccessLog
	{
		private String ip;
		//private String time;
		//private String method;
		//private String status;
		private int size;
		private boolean valid;
		
		public ApacheAccessLog(String record) {
			String[] str = record.split(" - - ");
			if(str.length==2)
			{
				this.ip=str[0].trim();
				if(isValidIPAddress(this.ip))
				{
					String s=str[1].trim();
					s=s.substring(s.lastIndexOf(' ')+1);
					if(isInteger(s,10))
					{
						this.size=Integer.parseInt(s);
						this.valid=true;
						return;
					}
				}
			}
			
			this.valid=false;
		}
		
		public boolean IsValid()
		{
			return this.valid;
		}
		
		public String getIpAddress()
		{
			return this.ip;
		}
		
		public int getSize()
		{
			return this.size;
		}
		
		private static boolean isValidIPAddress(String ip) {

			// Regex for digit from 0 to 255.
			String zeroTo255 = "(\\d{1,2}|(0|1)\\" + "d{2}|2[0-4]\\d|25[0-5])";

			// Regex for a digit from 0 to 255 and
			// followed by a dot, repeat 4 times.
			// this is the regex to validate an IP address.
			String regex = zeroTo255 + "\\." + zeroTo255 + "\\." + zeroTo255
					+ "\\." + zeroTo255;

			// Compile the ReGex
			Pattern p = Pattern.compile(regex);

			// If the IP address is empty
			// return false
			if (ip == null) {
				return false;
			}

			// Pattern class contains matcher() method
			// to find matching between given IP address
			// and regular expression.
			Matcher m = p.matcher(ip);

			// Return if the IP address
			// matched the ReGex
			return m.matches();
		}

		private static boolean isInteger(String s, int radix) {
		    if(s.isEmpty()) return false;
		    for(int i = 0; i < s.length(); i++) {
		        if(i == 0 && s.charAt(i) == '-') {
		            if(s.length() == 1) return false;
		            else continue;
		        }
		        if(Character.digit(s.charAt(i),radix) < 0) return false;
		    }
		    return true;
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, MyAvgValuePair>
	{
		private HashMap<String,MyAvgValuePair> map;
		
		@Override
		public void setup(Context context)
		{
			map=new HashMap<String, MyAvgValuePair>();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			ApacheAccessLog aalog=new ApacheAccessLog(value.toString());
			if(aalog.IsValid())
			{
				if(!this.map.containsKey(aalog.getIpAddress()))
					this.map.put(aalog.getIpAddress(), new MyAvgValuePair(aalog.getSize(), 1));
				else
				{
					MyAvgValuePair v=this.map.get(aalog.getIpAddress());
					this.map.put(aalog.getIpAddress(), new MyAvgValuePair(v.getSize()+aalog.getSize(), v.getCount()+1));
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Text word = new Text();
			for(Entry<String, MyAvgValuePair> item:this.map.entrySet())
			{
				word.set(item.getKey());
				MyAvgValuePair val=item.getValue();
				context.write(word, val);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, MyAvgValuePair, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterable<MyAvgValuePair> values, Context context) throws IOException, InterruptedException
		{
			long count=0;
			long sum = 0; 
			for (MyAvgValuePair val : values)
			{
				sum += val.getSize();
				count+=val.getCount();
			}
			
			result.set(sum/count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AvarageComputationInMapperCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(AvarageComputationInMapperCombiner.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyAvgValuePair.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(2);
		
		Configuration conf = new Configuration();
		
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		
		// delete existing directory
		if (hdfs.exists(output)) {
		  hdfs.delete(output, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
