import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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

public class WordCountInMapperCombiner extends Configured implements Tool
{
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private HashMap<String, Integer> map;
		
		@Override
		public void setup(Context context)
		{
			map=new HashMap<String, Integer>();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				token = token.trim();
				if (!"".equals(token)) 
				{
					if(!this.map.containsKey(token))
						this.map.put(token, 1);
					else
					{
						this.map.put(token, this.map.get(token).intValue()+1);
					}
					
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Text word = new Text();
			IntWritable count = new IntWritable(1);
			for(Entry<String, Integer> item:this.map.entrySet())
			{
				word.set(item.getKey());
				count.set(item.getValue().intValue());
				context.write(word, count);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new WordCountInMapperCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCountInMapperCombiner.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

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
